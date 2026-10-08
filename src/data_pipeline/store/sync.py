"""Sync: download what the catalog declares and store what changed.

An entry never downloaded is asked for its full history (or from its `start`). An entry already
stored is asked from its last real period minus a revision window. Each batch a source yields is
stored as it arrives, so an interrupted run keeps what it already downloaded. A series that fails
keeps its previous data; the failure is recorded in the index.
"""

import contextlib
import dataclasses
import datetime
import json
import os
import pathlib
from collections.abc import Collection, Iterator, Mapping, Sequence
from typing import Any

import pandas as pd

from data_pipeline.store.catalog import check_catalog
from data_pipeline.store.errors import LockHeldError, QuotaExhaustedError
from data_pipeline.store.http import Client
from data_pipeline.store.model import (
    CatalogEntry,
    DocumentData,
    Frequency,
    Kind,
    Outcome,
    Request,
    SeriesData,
    TableData,
)
from data_pipeline.store.sources.base import Source
from data_pipeline.store.storage import (
    INDEX_DTYPES,
    KEY,
    KIND_DOCUMENT,
    KIND_SERIES,
    KIND_TABLE,
    OBS_DTYPES,
    Storage,
    TableSchema,
    append_changes,
    append_rows,
    append_versions,
    document_rows,
    drop_repeated_versions,
    latest,
    records,
    table_frame,
    typed,
)

LOCK_FILE = "sync.lock"
WINDOW_DAYS: Mapping[Frequency, int] = {Frequency.DAILY: 30, Frequency.WEEKLY: 91}
WINDOW_MONTHS: Mapping[Frequency, int] = {
    Frequency.MONTHLY: 24,
    Frequency.QUARTERLY: 36,
    Frequency.ANNUAL: 60,
}
NOT_RETURNED = "the source did not return this series in this run"
STATUS_OK = "ok"
STATUS_FAILED = "failed"
EXIT_OK = 0
EXIT_FAILURES = 1
EXIT_CONFIGURATION = 2
EXIT_QUOTA = 3

Row = dict[str, Any]
LastReal = tuple[str, datetime.date]  # (period label, last day) of a series' last real observation


def window_start(last: datetime.date, frequency: Frequency) -> datetime.date:
    """First date asked again: 30 days (daily), 13 weeks (weekly), 24 months (monthly),
    36 months (quarterly) or 60 months (annual) before the last stored real period."""
    if frequency in WINDOW_DAYS:
        return last - datetime.timedelta(days=WINDOW_DAYS[frequency])
    months = last.year * 12 + last.month - 1 - WINDOW_MONTHS[frequency]
    return datetime.date(months // 12, months % 12 + 1, 1)


@dataclasses.dataclass(frozen=True)
class SourceReport:
    source: str
    ok: int
    failed: tuple[tuple[str, str], ...]  # (key, reason)
    new: int
    revised: int
    calls: int
    quota_exhausted: bool
    pending: int

    @property
    def requested(self) -> int:
        return self.ok + len(self.failed) + self.pending

    def lines(self) -> list[str]:
        """ASCII lines for the console."""
        label = f"{self.source:<12}"
        counts = f"{self.new} new, {self.revised} revised, {self.calls} calls"
        if not self.failed and not self.quota_exhausted:
            return [f"[ok]  {label}{self.ok} series, {counts}"]
        reasons = {reason for _, reason in self.failed}
        if self.ok == 0 and len(reasons) == 1 and not self.quota_exhausted:
            return [f"[x]   {label}{reasons.pop()}"]
        head = f"[x]   {label}{self.ok} of {self.requested} series, {counts}"
        if self.quota_exhausted:
            head += f"; quota exhausted, {self.pending} pending"
        return [head, *(f"      {key}  {reason}" for key, reason in self.failed)]


@dataclasses.dataclass(frozen=True)
class SyncReport:
    started_at: datetime.datetime
    sources: tuple[SourceReport, ...]

    @property
    def exit_code(self) -> int:
        if any(report.failed for report in self.sources):
            return EXIT_FAILURES
        if any(report.quota_exhausted for report in self.sources):
            return EXIT_QUOTA
        return EXIT_OK

    def lines(self) -> list[str]:
        return [line for report in self.sources for line in report.lines()]


@contextlib.contextmanager
def lock(root: pathlib.Path) -> Iterator[None]:
    """Hold <root>/sync.lock for the duration of a sync. A second sync is refused."""
    path = root / LOCK_FILE
    root.mkdir(parents=True, exist_ok=True)
    try:
        handle = path.open("x", encoding="ascii")
    except FileExistsError:
        msg = f"another sync is running, or one died: {path} exists. Delete it by hand if no sync is running."
        raise LockHeldError(msg) from None
    with handle:
        handle.write(str(os.getpid()))
    try:
        yield
    finally:
        path.unlink(missing_ok=True)


def observation_rows(series: SeriesData, fetched_at: datetime.datetime, today: datetime.date) -> list[Row]:
    """A period still open (its last day is after today) is never data: it is stored as a projection."""
    return [
        {
            "key": series.key,
            "period": observation.period,
            "date": pd.Timestamp(observation.date),
            "value": observation.value,
            "projection": observation.projection or observation.date > today,
            "fetched_at": fetched_at,
            "published_at": observation.published_at,
        }
        for observation in series.observations
    ]


def _append_observations(old: pd.DataFrame, new: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
    """Store what changed. A row the source dated is one version of its period, kept once and
    only when it says something new; a row without a date replaces the period's latest value only
    when that value (or its projection flag) changed."""
    dated = new["published_at"].notna().to_numpy()
    merged, added, revised = append_changes(old, new[~dated])
    fresh = drop_repeated_versions(merged, new[dated])
    merged, more, changed = append_versions(merged, fresh, KEY, ["value", "projection"])
    return merged, added + more, revised + changed


def last_real(observations: pd.DataFrame, today: datetime.date) -> dict[str, LastReal]:
    """Per key, (period, date) of the last observation that is a number, not a projection and closed."""
    if observations.empty:
        return {}
    current = latest(observations)
    closed = (current["date"] <= pd.Timestamp(today)).to_numpy()
    real = current[current["value"].notna().to_numpy() & ~current["projection"].astype(bool).to_numpy() & closed]
    last = real.sort_values("date", kind="stable").drop_duplicates("key", keep="last")
    return {
        str(key): (str(period), pd.Timestamp(date).date())
        for key, period, date in zip(last["key"], last["period"], last["date"], strict=True)
    }


def _since(entry: CatalogEntry, row: Row | None, last: LastReal | None) -> datetime.date | None:
    if row is None or last is None or not row.get("frequency"):
        return entry.start
    return window_start(last[1], Frequency(str(row["frequency"])))


def _ok_row(series: SeriesData, previous: Row | None, last: LastReal | None, now: datetime.datetime) -> Row:
    entry = series.entry
    first = previous.get("first_fetched_at") if previous else None
    return {
        "key": series.key,
        "alias": entry.alias,
        "source": entry.source,
        "source_id": entry.source_id,
        "name": entry.name or series.name,
        "country": series.country,
        "frequency": series.frequency.value,
        "units": series.units,
        "seasonal_adjustment": series.seasonal_adjustment,
        "stale_after_days": entry.stale_after_days,
        "attrs": json.dumps(dict(entry.attrs), sort_keys=True),
        "first_fetched_at": first if first is not None and pd.notna(first) else now,
        "last_fetched_at": now,
        "last_period": last[0] if last else None,
        "last_date": pd.Timestamp(last[1]) if last else None,
        "status": STATUS_OK,
        "reason": "",
        "kind": KIND_SERIES,
    }


def _failed_row(entry: CatalogEntry, previous: Row | None, reason: str, kind: str = KIND_SERIES) -> Row:
    if previous is not None:  # keep its provenance and data, record the failure
        return {**previous, "alias": entry.alias, "status": STATUS_FAILED, "reason": reason}
    return {
        "key": entry.key,
        "alias": entry.alias,
        "source": entry.source,
        "source_id": entry.source_id,
        "name": "",
        "country": "",
        "frequency": "",
        "units": "",
        "seasonal_adjustment": "",
        "stale_after_days": entry.stale_after_days,
        "attrs": json.dumps(dict(entry.attrs), sort_keys=True),
        "first_fetched_at": None,
        "last_fetched_at": None,
        "last_period": None,
        "last_date": None,
        "status": STATUS_FAILED,
        "reason": reason,
        "kind": kind,
    }


def held_periods(table: pd.DataFrame) -> frozenset[tuple[str, str]]:
    """The (frequency, period) pairs a stored table holds, whatever their version."""
    if table.empty or "frequency" not in table.columns or "period" not in table.columns:
        return frozenset()
    return frozenset(zip(table["frequency"].astype(str), table["period"].astype(str), strict=True))


def _table_row(table: TableData, previous: Row | None, stored: pd.DataFrame, now: datetime.datetime) -> Row:
    """The index row of a table: its newest period is the newest of its highest frequency, or
    its newest date when the table has no frequency."""
    entry = table.entry
    first = previous.get("first_fetched_at") if previous else None
    frequency = ""
    last_period = None
    last_date = None
    if not stored.empty and "frequency" not in stored.columns:
        last_date = pd.Timestamp(stored["date"].max())
        last_period = last_date.date().isoformat()
    elif not stored.empty:
        present = set(stored["frequency"].astype(str))
        frequency = next((item.value for item in Frequency if item.value in present), "")
        newest = stored[stored["frequency"] == frequency].sort_values("date", kind="stable").iloc[-1]
        last_period, last_date = str(newest["period"]), pd.Timestamp(newest["date"])
    return {
        "key": table.key,
        "alias": entry.alias,
        "source": entry.source,
        "source_id": entry.source_id,
        "name": entry.name or table.name,
        "country": "",
        "frequency": frequency,
        "units": "",
        "seasonal_adjustment": "",
        "stale_after_days": entry.stale_after_days if entry.stale_after_days is not None else table.stale_after_days,
        "attrs": json.dumps({**table.attrs, **entry.attrs}, sort_keys=True),
        "first_fetched_at": first if first is not None and pd.notna(first) else now,
        "last_fetched_at": now,
        "last_period": last_period,
        "last_date": last_date,
        "status": STATUS_OK,
        "reason": "",
        "kind": KIND_TABLE,
    }


def _sync_tables(
    storage: Storage,
    source: Source,
    client: Client,
    wanted: Sequence[CatalogEntry],
    index: dict[str, Row],
    spent_today: int,
    now: datetime.datetime,
    *,
    full: bool,
) -> SourceReport:
    """Sync a source of kind table. There is no `since`: each request carries what is stored."""
    name = source.name
    requests = [
        Request(entry, held=frozenset() if full else held_periods(storage.read_table(name, entry.source_id)))
        for entry in wanted
    ]
    calls_before = client.calls.get(name, 0)
    remaining = None if source.daily_budget is None else source.daily_budget - spent_today
    stored: set[str] = set()
    failed: dict[str, str] = {}
    added = revised = 0
    quota = remaining is not None and remaining <= 0
    if not quota:
        try:
            for batch in source.fetch(requests):
                for table in batch.tables:
                    existing = storage.read_table(name, table.entry.source_id)
                    schema = TableSchema(
                        table.key_columns, table.value_columns, table.attribute_columns, table.versioned
                    )
                    received = table_frame(table.rows, schema, now)
                    merge = append_versions if table.versioned else append_rows
                    merged, more, changed = merge(existing, received, table.key_columns, table.value_columns)
                    if merged is not existing:
                        storage.write_table(name, table.entry.source_id, merged, schema)
                    added += more
                    revised += changed
                    stored.add(table.key)
                    if table.key not in failed:
                        index[table.key] = _table_row(table, index.get(table.key), merged, now)
                for failure in batch.failures:
                    key = failure.entry.key
                    reason = f"{failure.outcome.value}: {failure.reason}"
                    failed.setdefault(key, reason)
                    index[key] = _failed_row(failure.entry, index.get(key), failed[key], KIND_TABLE)
                storage.write_index(typed(list(index.values()), INDEX_DTYPES))
                if remaining is not None and client.calls.get(name, 0) - calls_before >= remaining:
                    quota = True
                    break
        except QuotaExhaustedError:
            quota = True
    untouched = [request.entry for request in requests if request.entry.key not in stored | set(failed)]
    if not quota:
        for entry in untouched:
            failed[entry.key] = f"{Outcome.NOT_FOUND.value}: {NOT_RETURNED}"
            index[entry.key] = _failed_row(entry, index.get(entry.key), failed[entry.key], KIND_TABLE)
        if untouched:
            storage.write_index(typed(list(index.values()), INDEX_DTYPES))
    return SourceReport(
        source=name,
        ok=len(stored - set(failed)),
        failed=tuple(failed.items()),
        new=added,
        revised=revised,
        calls=client.calls.get(name, 0) - calls_before,
        quota_exhausted=quota,
        pending=len(untouched) if quota else 0,
    )


def _document_row(data: DocumentData, previous: Row | None, listed: pd.DataFrame, now: datetime.datetime) -> Row:
    """The index row of an entry's documents: its last period is the day of the newest one."""
    entry = data.entry
    first = previous.get("first_fetched_at") if previous else None
    last_date = None if listed.empty else pd.Timestamp(listed["date"].max())
    return {
        "key": data.key,
        "alias": entry.alias,
        "source": entry.source,
        "source_id": entry.source_id,
        "name": entry.name or data.name,
        "country": "",
        "frequency": "",
        "units": "",
        "seasonal_adjustment": "",
        "stale_after_days": entry.stale_after_days if entry.stale_after_days is not None else data.stale_after_days,
        "attrs": json.dumps({**data.attrs, **entry.attrs}, sort_keys=True),
        "first_fetched_at": first if first is not None and pd.notna(first) else now,
        "last_fetched_at": now,
        "last_period": None if last_date is None else last_date.date().isoformat(),
        "last_date": last_date,
        "status": STATUS_OK,
        "reason": "",
        "kind": KIND_DOCUMENT,
    }


def _sync_documents(
    storage: Storage,
    source: Source,
    client: Client,
    wanted: Sequence[CatalogEntry],
    index: dict[str, Row],
    spent_today: int,
    now: datetime.datetime,
) -> SourceReport:
    """Sync a source of kind document. Each request carries the documents already stored; a
    stored document is never asked for again, so `full` means nothing here."""
    name = source.name
    requests = [
        Request(entry, groups=frozenset(storage.read_documents(name, entry.source_id)["group"].astype(str)))
        for entry in wanted
    ]
    calls_before = client.calls.get(name, 0)
    remaining = None if source.daily_budget is None else source.daily_budget - spent_today
    stored: set[str] = set()
    failed: dict[str, str] = {}
    added = 0
    quota = remaining is not None and remaining <= 0
    if not quota:
        try:
            for batch in source.fetch(requests):
                for data in batch.documents:
                    identifier = data.entry.source_id
                    listed = storage.read_documents(name, identifier)
                    for document in data.documents:
                        for file in document.files:
                            storage.write_document(name, identifier, document.group, file.name, file.content)
                        files = [(file.name, file.role, file.url, file.content) for file in document.files]
                        rows = document_rows(document.group, document.date, files, document.attributes, now)
                        listed = rows if listed.empty else pd.concat([listed, rows], ignore_index=True)
                        added += len(rows)
                    if data.documents:
                        storage.write_documents(name, identifier, listed)
                    stored.add(data.key)
                    if data.key not in failed:
                        index[data.key] = _document_row(data, index.get(data.key), listed, now)
                for failure in batch.failures:
                    key = failure.entry.key
                    reason = f"{failure.outcome.value}: {failure.reason}"
                    failed.setdefault(key, reason)
                    index[key] = _failed_row(failure.entry, index.get(key), failed[key], KIND_DOCUMENT)
                storage.write_index(typed(list(index.values()), INDEX_DTYPES))
                if remaining is not None and client.calls.get(name, 0) - calls_before >= remaining:
                    quota = True
                    break
        except QuotaExhaustedError:
            quota = True
    untouched = [request.entry for request in requests if request.entry.key not in stored | set(failed)]
    if not quota:
        for entry in untouched:
            failed[entry.key] = f"{Outcome.NOT_FOUND.value}: {NOT_RETURNED}"
            index[entry.key] = _failed_row(entry, index.get(entry.key), failed[entry.key], KIND_DOCUMENT)
        if untouched:
            storage.write_index(typed(list(index.values()), INDEX_DTYPES))
    return SourceReport(
        source=name,
        ok=len(stored - set(failed)),
        failed=tuple(failed.items()),
        new=added,
        revised=0,
        calls=client.calls.get(name, 0) - calls_before,
        quota_exhausted=quota,
        pending=len(untouched) if quota else 0,
    )


def _calls_today(record: Mapping[str, Any] | None, today: datetime.date) -> int:
    budget = (record or {}).get("budget") or {}
    return int(budget.get("calls", 0)) if budget.get("day") == today.isoformat() else 0


def _sync_source(
    storage: Storage,
    source: Source,
    client: Client,
    wanted: Sequence[CatalogEntry],
    index: dict[str, Row],
    spent_today: int,
    now: datetime.datetime,
    *,
    full: bool,
) -> SourceReport:
    if source.kind is Kind.TABLE:
        return _sync_tables(storage, source, client, wanted, index, spent_today, now, full=full)
    if source.kind is Kind.DOCUMENT:
        return _sync_documents(storage, source, client, wanted, index, spent_today, now)
    name = source.name
    today = now.date()
    observations = storage.read_observations(name)
    known = last_real(observations, today)
    requests = [
        Request(entry, None if full else _since(entry, index.get(entry.key), known.get(entry.key)))
        for entry in wanted
    ]
    calls_before = client.calls.get(name, 0)
    remaining = None if source.daily_budget is None else source.daily_budget - spent_today
    done: set[str] = set()
    failed: list[tuple[str, str]] = []
    ok = added = revised = 0
    quota = remaining is not None and remaining <= 0
    if not quota:
        try:
            for batch in source.fetch(requests):
                fresh = [series for series in batch.series if series.key not in done]
                rows = [row for series in fresh for row in observation_rows(series, now, today)]
                if rows:
                    observations, more, changed = _append_observations(observations, typed(rows, OBS_DTYPES))
                    added += more
                    revised += changed
                    storage.write_observations(name, observations)
                if fresh:
                    known = last_real(observations, today)
                for series in fresh:
                    done.add(series.key)
                    ok += 1
                    index[series.key] = _ok_row(series, index.get(series.key), known.get(series.key), now)
                for failure in batch.failures:
                    key = failure.entry.key
                    if key in done:
                        continue
                    done.add(key)
                    reason = f"{failure.outcome.value}: {failure.reason}"
                    failed.append((key, reason))
                    index[key] = _failed_row(failure.entry, index.get(key), reason)
                storage.write_index(typed(list(index.values()), INDEX_DTYPES))
                if remaining is not None and client.calls.get(name, 0) - calls_before >= remaining:
                    quota = True
                    break
        except QuotaExhaustedError:
            quota = True
    missing = [request.entry for request in requests if request.entry.key not in done]
    if not quota:
        for entry in missing:
            reason = f"{Outcome.NOT_FOUND.value}: {NOT_RETURNED}"
            failed.append((entry.key, reason))
            index[entry.key] = _failed_row(entry, index.get(entry.key), reason)
        if missing:
            storage.write_index(typed(list(index.values()), INDEX_DTYPES))
    return SourceReport(
        source=name,
        ok=ok,
        failed=tuple(failed),
        new=added,
        revised=revised,
        calls=client.calls.get(name, 0) - calls_before,
        quota_exhausted=quota,
        pending=len(missing) if quota else 0,
    )


def _release_moved_aliases(index: dict[str, Row], entries: Sequence[CatalogEntry]) -> bool:
    """Take each alias the catalog gives to a series away from any other stored series that still
    has it (the catalog moved it, say from a mirror to the publisher). True when one was taken."""
    owners = {entry.alias: entry.key for entry in entries if entry.alias is not None}
    moved = [
        key for key, row in index.items() if isinstance(row["alias"], str) and owners.get(row["alias"], key) != key
    ]
    for key in moved:
        index[key] = {**index[key], "alias": None}
    return bool(moved)


def sync(
    storage: Storage,
    entries: Sequence[CatalogEntry],
    sources: Mapping[str, Source],
    client: Client,
    now: datetime.datetime,
    *,
    only_sources: Collection[str] = (),
    only_keys: Collection[str] = (),
    full: bool = False,
) -> SyncReport:
    """Run one sync. `now` (UTC) stamps every row fetched in this run."""
    with lock(storage.root):
        storage.prepare()
        check_catalog(entries, sources)
        index: dict[str, Row] = {str(row["key"]): row for row in records(storage.read_index())}
        if _release_moved_aliases(index, entries):
            storage.write_index(typed(list(index.values()), INDEX_DTYPES))
        runs = storage.read_runs()
        today = now.date()
        reports = []
        for name in sorted({entry.source for entry in entries}):
            if only_sources and name not in only_sources:
                continue
            wanted = [entry for entry in entries if entry.source == name and (not only_keys or entry.key in only_keys)]
            if not wanted:
                continue
            spent = _calls_today(runs.get(name), today)
            report = _sync_source(storage, sources[name], client, wanted, index, spent, now, full=full)
            reports.append(report)
            runs[name] = {
                "last_run": now.isoformat(),
                "ok": report.ok,
                "failed": len(report.failed),
                "new": report.new,
                "revised": report.revised,
                "calls": report.calls,
                "quota_exhausted": report.quota_exhausted,
                "pending": report.pending,
                "budget": {"day": today.isoformat(), "calls": spent + report.calls},
            }
            storage.write_runs(runs)
        return SyncReport(now, tuple(reports))
