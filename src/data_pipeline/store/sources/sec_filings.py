"""SEC filings: the documents a company files, as the files the SEC publishes.

One catalog id is one company, written as its ticker (`AAPL`). By default: forms 10-K, 10-Q,
8-K, 20-F and 40-F with their amendments, filed in the last 10 years. Each filing is one
document: its primary file and, for an 8-K, its exhibits (the earnings release is one).

A filing never changes at the SEC. Each request carries the accession numbers already stored;
the source lists the company's filings and downloads the ones that are missing, oldest first,
one filing a batch, so a run that stops resumes by itself. A filing that fails ends that
company's run; the next run asks for it again.
"""

import dataclasses
import datetime
import pathlib
import re
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from typing import Any

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store.errors import CatalogError, NetworkError, QuotaExhaustedError, RateLimitedError
from data_pipeline.store.http import Client
from data_pipeline.store.model import (
    CatalogEntry,
    Document,
    DocumentData,
    DocumentFile,
    Failure,
    FetchBatch,
    Kind,
    Outcome,
    Request,
)
from data_pipeline.store.sources.base import missing_key, reject_params, utc_today
from data_pipeline.store.sources.sec import (
    REQUESTS_PER_MINUTE,
    AnswerError,
    Edgar,
    check_ticker,
    resolve_tickers,
    unknown_ticker,
)

SUBMISSIONS_URL = "https://data.sec.gov/submissions/{name}"
ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{folder}/{file}"
FIELDS = ("forms", "amendments")
DEFAULT_FORMS = ("10-K", "10-Q", "8-K", "20-F", "40-F")
DEFAULT_YEARS = 10
AMENDMENT = "/A"
FORM = re.compile(r"^[0-9A-Z][0-9A-Z \-]{0,18}$")
EXHIBITS_OF = "8-K"
VIEWER_PAGE = re.compile(r"r\d+\.html?")  # a page of the SEC's XBRL viewer, not a document
PRIMARY = "primary"
EXHIBIT = "exhibit"
STALE_AFTER_DAYS = 140  # a quarter and slack


@dataclasses.dataclass(frozen=True, slots=True)
class Settings:
    """The fields of an entry, with their defaults filled in."""

    forms: frozenset[str]  # with the amendments when they are wanted
    start: datetime.date


@dataclasses.dataclass(frozen=True, slots=True)
class Filing:
    accession: str
    form: str
    filed: datetime.date
    period: str
    primary: str  # empty when the SEC names no primary document


def years_before(day: datetime.date, years: int) -> datetime.date:
    try:
        return day.replace(year=day.year - years)
    except ValueError:  # 29 February
        return day.replace(year=day.year - years, day=28)


def settings(entry: CatalogEntry, today: datetime.date) -> Settings:
    """Read and check the entry's fields. Raises CatalogError on anything this source rejects."""
    reject_params(entry, FIELDS)
    check_ticker(entry)
    forms = entry.params.get("forms", list(DEFAULT_FORMS))
    if not isinstance(forms, list | tuple) or not forms:
        msg = f"{entry.key}: 'forms' must be a non-empty list"
        raise CatalogError(msg)
    wanted = {str(form).upper() for form in forms}
    unknown = sorted(form for form in wanted if not FORM.match(form.removesuffix(AMENDMENT)))
    if unknown:
        msg = f"{entry.key}: not a SEC form: {', '.join(unknown)}"
        raise CatalogError(msg)
    amendments = entry.params.get("amendments", True)
    if not isinstance(amendments, bool):
        msg = f"{entry.key}: 'amendments' must be true or false"
        raise CatalogError(msg)
    if amendments:
        wanted |= {form + AMENDMENT for form in wanted if not form.endswith(AMENDMENT)}
    return Settings(frozenset(wanted), entry.start or years_before(today, DEFAULT_YEARS))


def read_filings(page: Mapping[str, Any]) -> list[Filing]:
    """The filings of one page of a company's list (the SEC sends one list per column)."""
    return [
        Filing(
            accession=str(accession),
            form=str(form),
            filed=datetime.date.fromisoformat(str(filed)),
            period=str(period or ""),
            primary=str(primary or ""),
        )
        for accession, form, filed, period, primary in zip(
            page["accessionNumber"],
            page["form"],
            page["filingDate"],
            page["reportDate"],
            page["primaryDocument"],
            strict=True,
        )
    ]


def missing(filings: Sequence[Filing], chosen: Settings, stored: Collection[str]) -> list[Filing]:
    """The filings to download: wanted form, filed since the start, not stored. Oldest first."""
    found = {
        filing.accession: filing
        for filing in filings
        if filing.form.upper() in chosen.forms and filing.filed >= chosen.start and filing.accession not in stored
    }
    return sorted(found.values(), key=lambda filing: (filing.filed, filing.accession))


def exhibits(items: Sequence[Mapping[str, Any]], primary: str) -> list[str]:
    """The exhibit documents of a filing's folder: every .htm that is not the primary document,
    an index page or a page of the XBRL viewer."""
    names = []
    for item in items:
        name = str(item.get("name") or "")
        low = name.lower()
        if not low.endswith((".htm", ".html")) or low == primary.lower() or "index" in low:
            continue
        if VIEWER_PAGE.fullmatch(low):
            continue
        names.append(name)
    return names


class SecFilings:
    name = "sec_filings"
    kind = Kind.DOCUMENT
    requests_per_minute = REQUESTS_PER_MINUTE
    daily_budget: int | None = None

    def __init__(
        self,
        client: Client,
        credentials: Credentials,
        today: Callable[[], datetime.date] = utc_today,
    ) -> None:
        self._client = client
        self._user_agent = credentials.get(keys.SEC_UA)
        self._today = today

    def validate(self, entry: CatalogEntry) -> None:
        settings(entry, self._today())

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
                yield from self._company(edgar, request, cik)
            except RateLimitedError as exc:
                raise QuotaExhaustedError(str(exc)) from exc
            except NetworkError as exc:
                yield FetchBatch(failures=(Failure(entry, Outcome.NETWORK_ERROR, str(exc)),))
            except AnswerError as exc:
                yield FetchBatch(failures=(Failure(entry, Outcome.SOURCE_ERROR, str(exc)),))
            except (KeyError, IndexError, TypeError, ValueError, AttributeError) as exc:
                reason = f"unexpected answer ({type(exc).__name__}: {exc})"
                yield FetchBatch(failures=(Failure(entry, Outcome.SOURCE_ERROR, reason),))

    def _company(self, edgar: Edgar, request: Request, cik: str) -> Iterator[FetchBatch]:
        """First a batch without documents, so a company with nothing new is not a failure;
        then one batch per filing."""
        entry = request.entry
        chosen = settings(entry, self._today())
        name, filings = self._filings(edgar, cik, chosen.start)

        def batch(*documents: Document) -> FetchBatch:
            data = DocumentData(
                entry=entry,
                key=entry.key,
                name=name or entry.source_id,
                documents=documents,
                stale_after_days=STALE_AFTER_DAYS,
                attrs={"cik": cik},
            )
            return FetchBatch(documents=(data,))

        yield batch()
        for filing in missing(filings, chosen, request.groups):
            yield batch(self._document(edgar, cik, filing))

    def _filings(self, edgar: Edgar, cik: str, start: datetime.date) -> tuple[str, list[Filing]]:
        """(company name, its filings). Older pages are asked for only when they reach `start`."""
        root = edgar.json(SUBMISSIONS_URL.format(name=f"CIK{cik}.json"))
        if root is None:
            msg = "the SEC has no list of filings for this company"
            raise AnswerError(msg)
        filings = read_filings(root["filings"]["recent"])
        for page in root["filings"].get("files") or []:
            if datetime.date.fromisoformat(str(page["filingTo"])) < start:
                continue
            older = edgar.json(SUBMISSIONS_URL.format(name=page["name"]))
            if older is None:
                msg = f"the SEC lists the page {page['name']} but does not have it"
                raise AnswerError(msg)
            filings.extend(read_filings(older))
        return str(root.get("name") or ""), filings

    def _document(self, edgar: Edgar, cik: str, filing: Filing) -> Document:
        folder = filing.accession.replace("-", "")
        primary = filing.primary or f"{filing.accession}.txt"  # no primary document: the complete filing
        names = [(primary, PRIMARY)]
        if filing.form.upper().startswith(EXHIBITS_OF):
            listing = edgar.json(ARCHIVE_URL.format(cik=int(cik), folder=folder, file="index.json"))
            items = [] if listing is None else listing["directory"]["item"]
            names.extend((name, EXHIBIT) for name in exhibits(items, pathlib.PurePosixPath(primary).name))
        files = []
        for name, role in names:
            url = ARCHIVE_URL.format(cik=int(cik), folder=folder, file=name)
            response = edgar.get(url)
            if response is None:
                msg = f"{filing.accession}: the SEC lists {name} but does not have it"
                raise AnswerError(msg)
            files.append(DocumentFile(pathlib.PurePosixPath(name).name, response.content, url, role))
        return Document(
            group=filing.accession,
            date=filing.filed,
            files=tuple(files),
            attributes={"form": filing.form, "period": filing.period},
        )
