"""SEC XBRL company facts: every fact a company has reported, as the SEC publishes it.

One catalog id is one company, written as its ticker (`AAPL`). The SEC's own number for it (CIK)
is read from the SEC's public list once per run. The SEC asks every client to identify itself:
`SEC_EDGAR_UA` holds a User-Agent such as `Name name@domain.com`.

One call brings the whole history of a company; there is no "since". The SEC lists one
appearance of a fact per filing that reports it, the original and every later filing that
repeats it as a comparative. Here a fact keeps its first appearance and each later one whose
value differs from the version before it; every version is dated with the day the SEC received
the filing. Nothing is mapped or derived: reading the concepts is the consumer's work.

A version describes the filing that first reported it (form, accession, filed, fiscal year and
period, which the SEC gives per filing), except for `frame`: the SEC sets it on one appearance
of a fact only, the latest filed, which is often a comparative, so a version takes it from
whichever of its appearances carries it. A version already stored without it gets it from the
next sync that brings it (see storage.fill_attributes): that adds no version.
"""

import datetime
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store.errors import NetworkError, QuotaExhaustedError, RateLimitedError
from data_pipeline.store.http import Client
from data_pipeline.store.model import CatalogEntry, Failure, FetchBatch, Kind, Outcome, Request, TableData
from data_pipeline.store.sources.base import missing_key, reject_params
from data_pipeline.store.sources.sec import (
    REQUESTS_PER_MINUTE,
    AnswerError,
    Edgar,
    check_ticker,
    resolve_tickers,
    unknown_ticker,
)

FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
KEY_COLUMNS = ("taxonomy", "concept", "unit", "start", "end")
VALUE_COLUMNS = ("value",)
ATTRIBUTE_COLUMNS = ("form", "accession", "filed", "fiscal_year", "fiscal_period", "frame")
STALE_AFTER_DAYS = 200  # a quarter, the filing deadline, and slack
TOLERANCE = 1e-9  # relative: the same number reported again is not a restatement
NO_FACTS = "the SEC has no XBRL facts for this company"

Row = dict[str, object]


def _text(value: object) -> str:
    return "" if value is None else str(value)


def _is_number(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def _same(before: float, after: float) -> bool:
    return abs(after - before) <= TOLERANCE * max(abs(before), abs(after))


def _appearances(items: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str], list[Mapping[str, Any]]]:
    """The appearances of one concept in one unit, by fact (start, end). An appearance without
    an end, without a filing day or without a number is not a fact and is skipped."""
    facts: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for item in items:
        if not item.get("end") or not item.get("filed") or not _is_number(item.get("val")):
            continue
        facts.setdefault((_text(item.get("start")), str(item["end"])), []).append(item)
    return facts


def versions(payload: Mapping[str, Any]) -> tuple[Row, ...]:
    """The versions of every fact of a company-facts answer, as table rows.

    Appearances of a fact are ordered by filing day, then accession number. The first is a
    version; a later one is a version only when its value differs from the version before it,
    and otherwise only lends the version its `frame` when it carries one.
    """
    rows: list[Row] = []
    for taxonomy, concepts in (payload.get("facts") or {}).items():
        for concept, node in concepts.items():
            for unit, items in (node.get("units") or {}).items():
                for (start, end), found in _appearances(items).items():
                    previous: float | None = None
                    current: Row = {}
                    for item in sorted(found, key=lambda item: (str(item["filed"]), _text(item.get("accn")))):
                        value = float(item["val"])
                        if previous is not None and _same(previous, value):
                            if item.get("frame"):
                                current["frame"] = _text(item["frame"])
                            continue
                        previous = value
                        filed = datetime.date.fromisoformat(str(item["filed"]))
                        current = {
                            "taxonomy": str(taxonomy),
                            "concept": str(concept),
                            "unit": str(unit),
                            "start": start,
                            "end": end,
                            "date": datetime.date.fromisoformat(end),
                            "value": value,
                            "form": _text(item.get("form")),
                            "accession": _text(item.get("accn")),
                            "filed": filed.isoformat(),
                            "fiscal_year": _text(item.get("fy")),
                            "fiscal_period": _text(item.get("fp")),
                            "frame": _text(item.get("frame")),
                            # the SEC gives the day it received the filing, not the hour: known
                            # from the end of that day, so no moment within it sees it early
                            "published_at": datetime.datetime.combine(filed, datetime.time.max, datetime.UTC),
                        }
                        rows.append(current)
    return tuple(rows)


class SecXbrl:
    name = "sec_xbrl"
    kind = Kind.TABLE
    requests_per_minute = REQUESTS_PER_MINUTE
    daily_budget: int | None = None

    def __init__(self, client: Client, credentials: Credentials) -> None:
        self._client = client
        self._user_agent = credentials.get(keys.SEC_UA)

    def validate(self, entry: CatalogEntry) -> None:
        reject_params(entry)
        check_ticker(entry)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        if not self._user_agent:
            yield missing_key(requests, keys.SEC_UA)
            return
        edgar = Edgar(self._client, self.name, self._user_agent)
        ciks = resolve_tickers(edgar, requests)
        if isinstance(ciks, FetchBatch):
            yield ciks
            return
        for request in requests:
            entry = request.entry
            cik = ciks.get(entry.source_id)
            if cik is None:
                yield FetchBatch(failures=(Failure(entry, Outcome.NOT_FOUND, unknown_ticker(entry)),))
                continue
            try:
                yield self._company(edgar, entry, cik)
            except RateLimitedError as exc:
                raise QuotaExhaustedError(str(exc)) from exc
            except NetworkError as exc:
                yield FetchBatch(failures=(Failure(entry, Outcome.NETWORK_ERROR, str(exc)),))
            except AnswerError as exc:
                yield FetchBatch(failures=(Failure(entry, Outcome.SOURCE_ERROR, str(exc)),))
            except (KeyError, IndexError, TypeError, ValueError, AttributeError) as exc:
                reason = f"unexpected answer ({type(exc).__name__}: {exc})"
                yield FetchBatch(failures=(Failure(entry, Outcome.SOURCE_ERROR, reason),))

    def _company(self, edgar: Edgar, entry: CatalogEntry, cik: str) -> FetchBatch:
        payload = edgar.json(FACTS_URL.format(cik=cik))
        if payload is None:
            return FetchBatch(failures=(Failure(entry, Outcome.NOT_FOUND, NO_FACTS),))
        table = TableData(
            entry=entry,
            key=entry.key,
            name=_text(payload.get("entityName")) or entry.source_id,
            rows=versions(payload),
            key_columns=KEY_COLUMNS,
            value_columns=VALUE_COLUMNS,
            stale_after_days=STALE_AFTER_DAYS,
            attribute_columns=ATTRIBUTE_COLUMNS,
            versioned=True,
            attrs={"cik": cik},
        )
        return FetchBatch(tables=(table,))
