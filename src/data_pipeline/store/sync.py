"""Sync: download what the catalog declares and store what changed.

An entry never downloaded is asked for its full history (or from its `start`). An entry already
stored is asked from its last real period minus a revision window. What a source of series
yields is stored at checkpoints (every FLUSH_SECONDS, at the end of the source, and when the run
is interrupted), so an interrupted run keeps what it already downloaded. A series that fails
keeps its previous data; the failure is recorded in the index. A source that breaks (a bug, a
damaged file) fails its own unfinished entries and the run goes on with the next source; its
calls are recorded in runs.json even when the run is interrupted.
"""

import contextlib
import dataclasses
import datetime
import json
import os
import pathlib
import socket
import sys
import time
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from typing import Any

import pandas as pd

from data_pipeline.store.catalog import check_catalog
from data_pipeline.store.errors import LockHeldError, QuotaExhaustedError, StoreError
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

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl

LOCK_FILE = "sync.lock"
LOCKED_BYTE = 1 << 30  # Windows locks a byte range: one far past the holder's details keeps them readable
WINDOW_DAYS: Mapping[Frequency, int] = {Frequency.DAILY: 30, Frequency.WEEKLY: 91}
WINDOW_MONTHS: Mapping[Frequency, int] = {
    Frequency.MONTHLY: 24,
    Frequency.QUARTERLY: 36,
    Frequency.ANNUAL: 60,
}
NOT_RETURNED = "the source did not return this series in this run"
STOPPED = "the sync of this source stopped"
STATUS_OK = "ok"
STATUS_FAILED = "failed"
EXIT_OK = 0
EXIT_FAILURES = 1
EXIT_CONFIGURATION = 2
EXIT_QUOTA = 3
FLUSH_SECONDS = 60.0  # a sync stores what it downloaded at least this often

Row = dict[str, Any]
Clock = Callable[[], datetime.datetime]  # UTC now: what stamps each batch when it arrives
LastReal = tuple[str, datetime.date]  # (period label, last day) of a series' last real observation


def window_start(last: datetime.date, frequency: Frequency) -> datetime.date:
    """First date asked again: 30 days (daily), 13 weeks (weekly), 24 months (monthly),
    36 months (quarterly) or 60 months (annual) before the last stored real period."""
    if frequency in WINDOW_DAYS:
        return last - datetime.timedelta(days=WINDOW_DAYS[frequency])
    months = last.year * 12 + last.month - 1 - WINDOW_MONTHS[frequency]
    return datetime.date(months // 12, months % 12 + 1, 1)


def _stopped(error: Exception, client: Client) -> str:
    """The reason recorded for every entry a broken source did not finish."""
    return client.scrub(f"{Outcome.SOURCE_ERROR.value}: {STOPPED}: {type(error).__name__}: {error}")


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


def _try_lock(descriptor: int) -> bool:
    """Take the operating system's lock on an open lock file without waiting. The system releases
    it when the process ends, however it ends, so a sync that was killed never blocks the next."""
    try:
        if sys.platform == "win32":
            os.lseek(descriptor, LOCKED_BYTE, os.SEEK_SET)
            msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
        else:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return False
    return True


def _unlock(descriptor: int) -> None:
    with contextlib.suppress(OSError):
        if sys.platform == "win32":
            os.lseek(descriptor, LOCKED_BYTE, os.SEEK_SET)
            msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
        else:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    os.close(descriptor)


def _holder(path: pathlib.Path) -> str:
    """Who holds the lock, as its file says: ' (pid 123 on HOST since ...)', or '' when unreadable."""
    try:
        found = json.loads(path.read_text(encoding="utf-8"))
        return f" (pid {found['pid']} on {found['host']} since {found['started']})"
    except (OSError, ValueError, TypeError, KeyError):
        return ""


@contextlib.contextmanager
def lock(root: pathlib.Path) -> Iterator[None]:
    """Hold <root>/sync.lock for the duration of a sync. A second sync is refused while the first
    runs. The file records the holder (pid, host, start); the lock itself is the operating
    system's, so a file left behind by a sync that died is taken over."""
    path = root / LOCK_FILE
    root.mkdir(parents=True, exist_ok=True)
    while True:
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
        if not _try_lock(descriptor):
            os.close(descriptor)
            msg = f"another sync is running on this store{_holder(path)}: {path}"
            raise LockHeldError(msg)
        try:
            same = os.fstat(descriptor).st_ino == path.stat().st_ino
        except FileNotFoundError:
            same = False
        if same:
            break
        _unlock(descriptor)  # the sync that held it removed the file meanwhile: lock the new one
    holder = {
        "pid": os.getpid(),
        "host": socket.gethostname(),
        "started": datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds"),
    }
    os.ftruncate(descriptor, 0)
    os.lseek(descriptor, 0, os.SEEK_SET)
    os.write(descriptor, json.dumps(holder).encode("ascii"))
    try:
        yield
    finally:
        _unlock(descriptor)
        with contextlib.suppress(OSError):  # on Windows a sync that just opened it keeps the file
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


def _frequency_change(series: SeriesData, previous: Row | None) -> str:
    """The failure of a stored series that the source now sends with another frequency, or "".

    Its periods are not stored: months next to quarters under one key would both stay current
    (no new month replaces an old one), so series() would mix them and frame() repeat dates.
    """
    held = str(previous.get("frequency") or "") if previous else ""
    if not held or held == series.frequency.value:
        return ""
    return (
        f"{Outcome.SOURCE_ERROR.value}: the source now sends this series as {series.frequency.value}; "
        f"the store holds it as {held} and keeps that data. One key cannot hold two frequencies"
    )


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
    clock: Clock,
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
    stopped = ""
    quota = remaining is not None and remaining <= 0
    if not quota:
        try:
            for batch in source.fetch(requests):
                received_at = clock()
                for table in batch.tables:
                    existing = storage.read_table(name, table.entry.source_id)
                    schema = TableSchema(
                        table.key_columns, table.value_columns, table.attribute_columns, table.versioned
                    )
                    received = table_frame(table.rows, schema, received_at)
                    merge = append_versions if table.versioned else append_rows
                    merged, more, changed = merge(existing, received, table.key_columns, table.value_columns)
                    if merged is not existing:
                        storage.write_table(name, table.entry.source_id, merged, schema)
                    added += more
                    revised += changed
                    stored.add(table.key)
                    if table.key not in failed:
                        index[table.key] = _table_row(table, index.get(table.key), merged, received_at)
                for failure in batch.failures:
                    key = failure.entry.key
                    reason = client.scrub(f"{failure.outcome.value}: {failure.reason}")
                    failed.setdefault(key, reason)
                    index[key] = _failed_row(failure.entry, index.get(key), failed[key], KIND_TABLE)
                storage.write_index(typed(list(index.values()), INDEX_DTYPES))
                if remaining is not None and client.calls.get(name, 0) - calls_before >= remaining:
                    quota = True
                    break
        except QuotaExhaustedError:
            quota = True
        except Exception as exc:  # noqa: BLE001 - a broken source fails its own entries, not the run
            stopped = _stopped(exc, client)
    untouched = [request.entry for request in requests if request.entry.key not in stored | set(failed)]
    if not quota:
        for entry in untouched:
            failed[entry.key] = stopped or f"{Outcome.NOT_FOUND.value}: {NOT_RETURNED}"
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
    clock: Clock,
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
    stopped = ""
    quota = remaining is not None and remaining <= 0
    if not quota:
        try:
            for batch in source.fetch(requests):
                received_at = clock()
                for data in batch.documents:
                    identifier = data.entry.source_id
                    listed = storage.read_documents(name, identifier)
                    for document in data.documents:
                        for file in document.files:
                            storage.write_document(name, identifier, document.group, file.name, file.content)
                        files = [(file.name, file.role, file.url, file.content) for file in document.files]
                        rows = document_rows(document.group, document.date, files, document.attributes, received_at)
                        listed = rows if listed.empty else pd.concat([listed, rows], ignore_index=True)
                        added += len(rows)
                    if data.documents:
                        storage.write_documents(name, identifier, listed)
                    stored.add(data.key)
                    if data.key not in failed:
                        index[data.key] = _document_row(data, index.get(data.key), listed, received_at)
                for failure in batch.failures:
                    key = failure.entry.key
                    reason = client.scrub(f"{failure.outcome.value}: {failure.reason}")
                    failed.setdefault(key, reason)
                    index[key] = _failed_row(failure.entry, index.get(key), failed[key], KIND_DOCUMENT)
                storage.write_index(typed(list(index.values()), INDEX_DTYPES))
                if remaining is not None and client.calls.get(name, 0) - calls_before >= remaining:
                    quota = True
                    break
        except QuotaExhaustedError:
            quota = True
        except Exception as exc:  # noqa: BLE001 - a broken source fails its own entries, not the run
            stopped = _stopped(exc, client)
    untouched = [request.entry for request in requests if request.entry.key not in stored | set(failed)]
    if not quota:
        for entry in untouched:
            failed[entry.key] = stopped or f"{Outcome.NOT_FOUND.value}: {NOT_RETURNED}"
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


@dataclasses.dataclass
class _Checkpoints:
    """Series downloaded from one source wait in memory and are stored together: every
    FLUSH_SECONDS, when the source ends, and when the run is interrupted. Each checkpoint merges
    and writes the source's file and the index once, however many batches it holds. Every series
    is stamped with the time it arrived, not the time of the checkpoint or of the run's start."""

    storage: Storage
    name: str
    index: dict[str, Row]
    observations: pd.DataFrame
    clock: Clock
    monotonic: Callable[[], float]
    rows: list[Row] = dataclasses.field(default_factory=list)
    series: list[tuple[SeriesData, datetime.datetime]] = dataclasses.field(default_factory=list)
    changed_index: bool = False
    added: int = 0
    revised: int = 0
    last: float = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.last = self.monotonic()

    def keep(self, series: Sequence[SeriesData]) -> None:
        """Hold downloaded series until the next checkpoint."""
        received_at = self.clock()
        for item in series:
            self.rows.extend(observation_rows(item, received_at, received_at.date()))
            self.series.append((item, received_at))

    def failed(self, entry: CatalogEntry, reason: str) -> None:
        self.index[entry.key] = _failed_row(entry, self.index.get(entry.key), reason)
        self.changed_index = True

    def tick(self) -> None:
        """Store what is held when the last checkpoint is FLUSH_SECONDS old."""
        if self.monotonic() - self.last >= FLUSH_SECONDS:
            self.flush()

    def flush(self) -> None:
        if self.rows:
            self.observations, more, changed = _append_observations(self.observations, typed(self.rows, OBS_DTYPES))
            self.added += more
            self.revised += changed
            self.storage.write_observations(self.name, self.observations)
        if self.series:
            known = last_real(self.observations, self.clock().date())
            for item, received_at in self.series:
                self.index[item.key] = _ok_row(item, self.index.get(item.key), known.get(item.key), received_at)
        if self.series or self.changed_index:
            self.storage.write_index(typed(list(self.index.values()), INDEX_DTYPES))
        self.rows, self.series, self.changed_index = [], [], False
        self.last = self.monotonic()


def _sync_source(
    storage: Storage,
    source: Source,
    client: Client,
    wanted: Sequence[CatalogEntry],
    index: dict[str, Row],
    spent_today: int,
    clock: Clock,
    *,
    full: bool,
    monotonic: Callable[[], float] = time.monotonic,
) -> SourceReport:
    if source.kind is Kind.TABLE:
        return _sync_tables(storage, source, client, wanted, index, spent_today, clock, full=full)
    if source.kind is Kind.DOCUMENT:
        return _sync_documents(storage, source, client, wanted, index, spent_today, clock)
    name = source.name
    today = clock().date()
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
    ok = 0
    checkpoints = _Checkpoints(storage, name, index, observations, clock, monotonic)
    stopped = ""
    quota = remaining is not None and remaining <= 0
    if not quota:
        try:
            for batch in source.fetch(requests):
                fresh = [series for series in batch.series if series.key not in done]
                moved = {series.key: _frequency_change(series, index.get(series.key)) for series in fresh}
                checkpoints.keep([series for series in fresh if not moved[series.key]])
                for series in fresh:
                    done.add(series.key)
                    if moved[series.key]:
                        failed.append((series.key, moved[series.key]))
                        checkpoints.failed(series.entry, moved[series.key])
                    else:
                        ok += 1
                for failure in batch.failures:
                    key = failure.entry.key
                    if key in done:
                        continue
                    done.add(key)
                    reason = client.scrub(f"{failure.outcome.value}: {failure.reason}")
                    failed.append((key, reason))
                    checkpoints.failed(failure.entry, reason)
                checkpoints.tick()
                if remaining is not None and client.calls.get(name, 0) - calls_before >= remaining:
                    quota = True
                    break
        except QuotaExhaustedError:
            quota = True
        except Exception as exc:  # noqa: BLE001 - a broken source fails its own series, not the run
            stopped = _stopped(exc, client)
        finally:
            checkpoints.flush()  # also when the run is interrupted: keep what was downloaded
    missing = [request.entry for request in requests if request.entry.key not in done]
    if not quota:
        for entry in missing:
            reason = stopped or f"{Outcome.NOT_FOUND.value}: {NOT_RETURNED}"
            failed.append((entry.key, reason))
            index[entry.key] = _failed_row(entry, index.get(entry.key), reason)
        if missing:
            storage.write_index(typed(list(index.values()), INDEX_DTYPES))
    return SourceReport(
        source=name,
        ok=ok,
        failed=tuple(failed),
        new=checkpoints.added,
        revised=checkpoints.revised,
        calls=client.calls.get(name, 0) - calls_before,
        quota_exhausted=quota,
        pending=len(missing) if quota else 0,
    )


def _broken_source(
    source: Source,
    wanted: Sequence[CatalogEntry],
    index: dict[str, Row],
    before: Mapping[str, Row | None],
    reason: str,
    calls: int,
) -> SourceReport:
    """The report of a source that broke outside its downloads (say its file is damaged): every
    entry it did not finish in this run fails with `reason`; what it finished stays as recorded."""
    ok = 0
    failed: list[tuple[str, str]] = []
    for entry in wanted:
        row = index.get(entry.key)
        if row is not None and row is not before[entry.key]:  # finished in this run
            if row["status"] == STATUS_OK:
                ok += 1
            else:
                failed.append((entry.key, str(row["reason"])))
            continue
        index[entry.key] = _failed_row(entry, row, reason, source.kind.value)
        failed.append((entry.key, reason))
    return SourceReport(source.name, ok, tuple(failed), 0, 0, calls, False, 0)


def _run_record(
    previous: Mapping[str, Any] | None,
    report: SourceReport | None,
    now: datetime.datetime,
    budget: Mapping[str, Any],
) -> dict[str, Any]:
    """A source's entry in runs.json. Without a report (the run was interrupted), the previous
    entry is kept with the calls spent today, so the daily budget is never lost."""
    if report is None:
        return {**(previous or {}), "budget": dict(budget)}
    return {
        "last_run": now.isoformat(),
        "ok": report.ok,
        "failed": len(report.failed),
        "new": report.new,
        "revised": report.revised,
        "calls": report.calls,
        "quota_exhausted": report.quota_exhausted,
        "pending": report.pending,
        "budget": dict(budget),
    }


def _check_selection(
    entries: Sequence[CatalogEntry], only_sources: Collection[str], only_keys: Collection[str]
) -> None:
    """A source or key asked for that the catalog does not declare (a typo, say) is an error,
    not a run that syncs nothing and reports success."""
    declared = {entry.source for entry in entries}
    unknown = sorted(set(only_sources) - declared)
    if unknown:
        names = ", ".join(repr(name) for name in unknown)
        msg = f"no catalog entry has source {names} (sources in the catalog: {', '.join(sorted(declared)) or 'none'})"
        raise StoreError(msg)
    unknown = sorted(set(only_keys) - {entry.key for entry in entries})
    if unknown:
        msg = f"no catalog entry has key {', '.join(repr(key) for key in unknown)}"
        raise StoreError(msg)
    selected = [
        entry
        for entry in entries
        if (not only_sources or entry.source in only_sources) and (not only_keys or entry.key in only_keys)
    ]
    if (only_sources or only_keys) and not selected:
        msg = "the sources and keys asked for select no catalog entry together"
        raise StoreError(msg)


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
    monotonic: Callable[[], float] = time.monotonic,
    clock: Clock | None = None,
) -> SyncReport:
    """Run one sync. `now` (UTC) is the run's start, recorded in runs.json and the report;
    `clock` stamps each batch with the time it arrived (by default, always `now`); `monotonic`
    times the checkpoints."""
    stamp = clock or (lambda: now)
    _check_selection(entries, only_sources, only_keys)
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
            calls_before = client.calls.get(name, 0)
            before = {entry.key: index.get(entry.key) for entry in wanted}
            report = None
            try:
                report = _sync_source(
                    storage, sources[name], client, wanted, index, spent, stamp, full=full, monotonic=monotonic
                )
            except Exception as exc:  # noqa: BLE001 - one broken source must not stop the others
                calls = client.calls.get(name, 0) - calls_before
                report = _broken_source(sources[name], wanted, index, before, _stopped(exc, client), calls)
                storage.write_index(typed(list(index.values()), INDEX_DTYPES))
            finally:  # also when the run is interrupted: the calls spent today are never forgotten
                budget = {"day": today.isoformat(), "calls": spent + client.calls.get(name, 0) - calls_before}
                runs[name] = _run_record(runs.get(name), report, now, budget)
                storage.write_runs(runs)
            reports.append(report)
        return SyncReport(now, tuple(reports))
