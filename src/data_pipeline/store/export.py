"""Copies of the stored SEC filings beside the store: each filing's XBRL facts as a CSV named like
the filing (`AAPL_10-K_2025-09-27_xbrl.csv`), and each filing's HTML as Markdown under its stored
name (`AAPL_10-K_2025-09-27.md`).

The store keeps a company's XBRL facts as one table (sec_xbrl), every version dated with the
filing that reported it. `export_company` writes, for each filing of the company that the store
also holds (sec_filings), the versions dated to it, under the filing's label. The file holds the
facts the filing reported first and the ones whose value it changed; a value it only repeated as
a comparative stays with the filing that reported it first.

`export_text_company` writes each of a company's stored HTML files (the filing and its exhibits)
as Markdown, converted by data_pipeline.store.markdown: a format that file-sharing tools and
language models read, about a seventh of the HTML's size. Files that are not HTML are left out.

The copies go to a folder outside the store, one folder per company. A second export rewrites only
the files whose content changed. The store is never changed.
"""

import dataclasses
import io
import pathlib
from collections.abc import Sequence

import pandas as pd

from data_pipeline._files import write_atomic
from data_pipeline.store.markdown import to_markdown
from data_pipeline.store.model import check_name

FILINGS_SOURCE = "sec_filings"
XBRL_SOURCE = "sec_xbrl"
XBRL_SUFFIX = "_xbrl.csv"
TEXT_SUFFIX = ".md"
HTML_SUFFIXES = frozenset({".htm", ".html"})


class UnlabelledError(ValueError):
    """A filing stored without a readable name: the store needs `migrate` first."""


@dataclasses.dataclass(frozen=True, slots=True)
class CompanyExport:
    """What one company's export did."""

    ticker: str
    files: int  # files in the folder for the company's stored filings
    written: int  # of those, new or rewritten


def _text(value: object) -> str:
    return "" if value is None or pd.isna(value) else str(value)  # type: ignore[call-overload]


def labels(documents: pd.DataFrame) -> dict[str, str]:
    """accession -> label, for the filings of `documents` (what Store.documents returns)."""
    found: dict[str, str] = {}
    column = documents["label"] if "label" in documents.columns else pd.Series([""] * len(documents))
    for accession, label in zip(documents["group"].astype(str), column.map(_text), strict=True):
        if not label:
            msg = f"filing {accession} is stored without a readable name: run `data-pipeline migrate` first"
            raise UnlabelledError(msg)
        found[accession] = label
    return found


def _csv(frame: pd.DataFrame) -> bytes:
    buffer = io.StringIO()
    frame.to_csv(buffer, index=False, lineterminator="\n")
    return buffer.getvalue().encode("utf-8")


def export_company(ticker: str, documents: pd.DataFrame, facts: pd.DataFrame, folder: pathlib.Path) -> CompanyExport:
    """Write the XBRL facts of each of one company's stored filings. `facts` is its sec_xbrl table
    with every version and an `accession` column; it is written with the columns it has."""
    named = labels(documents)
    files = written = 0
    if facts.empty:
        return CompanyExport(ticker, files, written)
    folder.mkdir(parents=True, exist_ok=True)
    for accession, reported in facts.groupby(facts["accession"].astype(str), sort=True):
        label = named.get(str(accession))
        if label is None:  # a filing the store does not hold, older than its window, say
            continue
        name = label + XBRL_SUFFIX
        check_name(name)
        files += 1
        written += _write_if_changed(folder / name, _csv(reported.reset_index(drop=True)))
    return CompanyExport(ticker, files, written)


def _write_if_changed(target: pathlib.Path, content: bytes) -> bool:
    if target.exists() and target.read_bytes() == content:
        return False
    write_atomic(target, content)
    return True


def export_text_company(ticker: str, documents: pd.DataFrame, folder: pathlib.Path) -> CompanyExport:
    """Write each of one company's stored HTML files as Markdown, named as it is stored with `.md`
    for its extension. `documents` is what Store.documents returns, with `stored_as` and `path`."""
    files = written = 0
    column = documents["stored_as"] if "stored_as" in documents.columns else pd.Series([""] * len(documents))
    rows = zip(documents["group"].astype(str), column.map(_text), documents["path"], strict=True)
    for accession, stored_as, path in rows:
        if not stored_as:
            msg = f"filing {accession} is stored without a readable name: run `data-pipeline migrate` first"
            raise UnlabelledError(msg)
        stored = pathlib.PurePath(stored_as)
        if stored.suffix.lower() not in HTML_SUFFIXES:
            continue
        name = stored.stem + TEXT_SUFFIX
        check_name(name)
        content = to_markdown(pathlib.Path(path).read_bytes()).encode("utf-8")
        folder.mkdir(parents=True, exist_ok=True)
        files += 1
        written += _write_if_changed(folder / name, content)
    return CompanyExport(ticker, files, written)


def check_destination(root: pathlib.Path, destination: pathlib.Path) -> None:
    """Raise ValueError when `destination` is the store's folder or inside it: the export is a
    copy beside the store, never part of it."""
    store = root.resolve()
    target = destination.resolve()
    if target == store or store in target.parents:
        msg = f"{destination}: the export must go outside the store ({root})"
        raise ValueError(msg)


def lines(results: Sequence[CompanyExport]) -> list[str]:
    """The console report of an export, ASCII only."""
    out = [f"[ok]  {result.ticker}  {result.files} files, {result.written} new or changed" for result in results]
    out.append(f"[ok]  {len(results)} companies, {sum(result.files for result in results)} files")
    return out
