"""Move the SEC filings of a schema-1 store to their readable names (schema 2).

Schema 1 stored each file as published, in a folder per filing: `AAPL/0000320193-25-000079/
aapl-20250927.htm`. Schema 2 stores it under a readable name in the company's folder:
`AAPL/AAPL_10-K_2025-09-27.htm` (see sources.sec_filings). A schema-2 library reads a schema-1
file where it is, so migrating is a choice, not a need: it renames the files in place (nothing
is downloaded or copied), gives each row of the list of documents its `label` and `stored_as`,
and removes the filing folders it emptied.

Filings are named in the order they were filed, as a sync stores them, so a migrated store and
one downloaded afresh give the same names. The migration holds the sync lock. It can stop
anywhere and be run again: a file already at its new name is not moved twice, and the names do
not change, since the list of documents is written only once a company's files are all moved.
"""

import contextlib
import dataclasses

import pandas as pd

from data_pipeline.store.errors import StoreError
from data_pipeline.store.sources import sec_filings
from data_pipeline.store.storage import DOCUMENTS_DIR, Storage, readable_names, used_values
from data_pipeline.store.sync import lock

SOURCE = sec_filings.SecFilings.name


@dataclasses.dataclass(frozen=True, slots=True)
class Migrated:
    """What the migration did to one company."""

    ticker: str
    moved: int  # files renamed
    missing: int  # files the list names that were not on the disk: their rows get a name all the same


def _text(value: object) -> str:
    return "" if value is None or pd.isna(value) else str(value)  # type: ignore[call-overload]


def _legacy(listed: pd.DataFrame) -> pd.Series:
    """The rows of files stored as published."""
    if "stored_as" not in listed.columns:
        return pd.Series(True, index=listed.index)
    return listed["stored_as"].map(_text) == ""


def migrate_company(storage: Storage, ticker: str) -> Migrated | None:
    """Rename one company's schema-1 files; None when it has none."""
    listed = storage.read_documents(SOURCE, ticker)
    legacy = _legacy(listed)
    if not legacy.any():
        return None
    listed = listed.copy()
    for column in ("label", "stored_as"):
        listed[column] = listed[column].map(_text) if column in listed.columns else ""
    labels, names = used_values(listed, "label"), used_values(listed, "stored_as")
    order = listed[legacy].sort_values(["date", "group"], kind="stable")
    moved = missing = 0
    groups = []
    for group, rows in order.groupby("group", sort=False):
        first = rows.iloc[0]
        base = sec_filings.label(
            ticker, _text(first["form"]), _text(first["period"]), pd.Timestamp(first["date"]).date()
        )
        files = [(str(row.file), sec_filings.tag(str(row.file), str(row.role))) for row in rows.itertuples()]
        used, stored = readable_names(base, files, labels, names)
        labels.add(used)
        names.update(stored)
        for index, (file, _), target in zip(rows.index, files, stored, strict=True):
            old = storage.document_path(SOURCE, ticker, str(group), file)
            new = storage.document_path(SOURCE, ticker, str(group), file, target)
            if old.exists() and new.exists():
                msg = f"{new} already exists: move it aside, then migrate again to store {old} there"
                raise StoreError(msg)
            if old.exists():
                old.replace(new)
                moved += 1
            elif not new.exists():
                missing += 1
            listed.loc[index, ["label", "stored_as"]] = [used, target]
        groups.append(str(group))
    storage.write_documents(SOURCE, ticker, listed)
    for group in groups:
        with contextlib.suppress(OSError):  # a folder something else put a file in stays
            (storage.root / DOCUMENTS_DIR / SOURCE / ticker / group).rmdir()
    return Migrated(ticker, moved, missing)


def migrate(storage: Storage) -> list[Migrated]:
    """Rename the schema-1 SEC filings of the store and mark it schema 2."""
    with lock(storage.root):
        storage.prepare()
        results = [migrate_company(storage, ticker) for ticker in storage.document_names(SOURCE)]
    return [result for result in results if result is not None]


def lines(results: list[Migrated]) -> list[str]:
    """The console report of a migration, ASCII only."""
    if not results:
        return ["[ok]  nothing to migrate: every filing already has its readable name"]
    out = [f"[ok]  {result.ticker}  {result.moved} files renamed" for result in results]
    out.extend(
        f"[x]   {result.ticker}  {result.missing} files listed but not on the disk"
        for result in results
        if result.missing
    )
    out.append(f"[ok]  {len(results)} companies, {sum(result.moved for result in results)} files renamed")
    return out
