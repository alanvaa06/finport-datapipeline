"""Checking one key with a single request to its source, before it is saved.

A store source runs its usual download on one probe series, so the rules each source already
has for recognising a rejected key are reused; nothing is written to any store. The SEC gets
one request of its own. INEGI accepts any token, so it is not requested. A check never raises.
"""

import dataclasses
import datetime
import enum
from collections.abc import Iterator, Mapping, Sequence

import httpx

from data_pipeline import credentials as keys
from data_pipeline.credentials import Credentials
from data_pipeline.store.errors import QuotaExhaustedError, StoreError
from data_pipeline.store.http import Client, scrub
from data_pipeline.store.model import CatalogEntry, FetchBatch, Frequency, Outcome, Request
from data_pipeline.store.sources import create
from data_pipeline.store.sources.sec import TICKERS_URL, AnswerError, Edgar

PROBE_DAYS = 400  # a probe asks for about a year, so every source answers with a few points
CHECK_TIMEOUT = 15.0  # seconds: someone is waiting for the verdict, unlike a sync


class Verdict(enum.StrEnum):
    OK = "ok"  # the source answered with data
    REJECTED = "rejected"  # the source said the key is wrong
    UNKNOWN = "unknown"  # could not tell: no network, a 5xx, a quota, INEGI


MARKS: Mapping[Verdict, str] = {Verdict.OK: "[ok]", Verdict.REJECTED: "[x] ", Verdict.UNKNOWN: "[?] "}


@dataclasses.dataclass(frozen=True)
class Check:
    name: str
    verdict: Verdict
    detail: str

    def line(self) -> str:
        """One ASCII line for the console."""
        return f"{MARKS[self.verdict]}  {self.name}  {self.detail}".encode("ascii", "replace").decode("ascii")

    def as_dict(self) -> dict[str, str]:
        return {"name": self.name, "verdict": self.verdict.value, "detail": self.detail}


# INEGI is not probed: its BISE endpoint answers with real data for any token, "zzz" included
# (verified 2026-10-06), so a probe would report OK for a wrong token.
INEGI_DETAIL = "INEGI answers with any token, so it cannot be checked here"


def _probes(today: datetime.date) -> Mapping[str, tuple[str, CatalogEntry]]:
    """Key name -> (store source, one small entry that source answers with data)."""
    return {
        keys.FRED: ("fred", CatalogEntry("fred", "UNRATE")),
        keys.BLS: ("bls", CatalogEntry("bls", "CUUR0000SA0")),
        keys.BANXICO: ("banxico", CatalogEntry("banxico", "SF43718", frequency=Frequency.DAILY)),
        keys.COMTRADE: (
            "comtrade",
            CatalogEntry("comtrade", "MEX", params={"annual_from": today.year - 2, "months": 0}),
        ),
    }


PROBED: tuple[str, ...] = tuple(_probes(datetime.date(2000, 1, 1)))


def check_batches(name: str, batches: Iterator[FetchBatch]) -> Check:
    """The verdict of the first batch a source yields for a probe."""
    try:
        first = next(batches, None)
    except QuotaExhaustedError as exc:
        return Check(name, Verdict.UNKNOWN, f"could not check: {exc}")
    except Exception as exc:  # noqa: BLE001  # a check reports, it never raises
        return Check(name, Verdict.UNKNOWN, f"could not check: {type(exc).__name__}: {exc}")
    finally:
        close = getattr(batches, "close", None)
        if close is not None:
            close()
    if first is None:
        return Check(name, Verdict.UNKNOWN, "could not check: the source sent no answer")
    if first.failures:
        failure = first.failures[0]
        if failure.outcome is Outcome.KEY_ERROR:
            return Check(name, Verdict.REJECTED, failure.reason)
        return Check(name, Verdict.UNKNOWN, f"could not check: {failure.reason}")
    return Check(name, Verdict.OK, "works")


def _client(secrets: Sequence[str], transport: httpx.BaseTransport | None) -> Client:
    return Client(secrets=secrets, transport=transport, sleep=lambda _s: None, retries=0, timeout=CHECK_TIMEOUT)


def _check_source(name: str, value: str, transport: httpx.BaseTransport | None, today: datetime.date) -> Check:
    source_name, entry = _probes(today)[name]
    credentials = Credentials({name: value})
    with _client(credentials.secrets(), transport) as client:
        source = create(source_name, client, credentials)
        since = today - datetime.timedelta(days=PROBE_DAYS)
        return check_batches(name, source.fetch([Request(entry, since=since)]))


def _check_sec(value: str, transport: httpx.BaseTransport | None) -> Check:
    if "@" not in value:
        return Check(keys.SEC_UA, Verdict.REJECTED, "the SEC asks for a contact e-mail: 'Your Name you@domain.com'")
    with _client((), transport) as client:
        try:
            listed = Edgar(client, "sec_xbrl", value).json(TICKERS_URL)
        except QuotaExhaustedError as exc:
            return Check(keys.SEC_UA, Verdict.REJECTED, str(exc))
        except (StoreError, AnswerError, ValueError) as exc:
            return Check(keys.SEC_UA, Verdict.UNKNOWN, f"could not check: {exc}")
    if listed is None:
        return Check(keys.SEC_UA, Verdict.UNKNOWN, "could not check: the SEC answered 404")
    return Check(keys.SEC_UA, Verdict.OK, "works")


def check(
    name: str,
    value: str,
    *,
    transport: httpx.BaseTransport | None = None,
    today: datetime.date | None = None,
) -> Check:
    """Ask the source of `name` whether `value` works. Raises KeyError for an unknown name."""
    keys.key(name)
    value = value.strip()
    if not value:
        return Check(name, Verdict.REJECTED, "empty value")
    if name == keys.INEGI:
        result = Check(name, Verdict.UNKNOWN, INEGI_DETAIL)
    elif name == keys.SEC_UA:
        result = _check_sec(value, transport)
    else:
        result = _check_source(name, value, transport, today or datetime.datetime.now(datetime.UTC).date())
    return dataclasses.replace(
        result, detail=scrub(result.detail, (value,)) if keys.key(name).secret else result.detail
    )
