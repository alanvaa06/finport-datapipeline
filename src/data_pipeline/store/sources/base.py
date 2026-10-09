"""What every source shares: the `Source` protocol, the per-request runner and the parsing of
numbers with each source's missing-value sentinels.

Failure policy: a series that does not exist is recorded and skipped; a rejected credential
skips the rest of that source; a network failure or an unexpected answer fails only that
request; an exhausted quota stops the source. Nothing is filled in.
"""

import contextlib
import contextvars
import datetime
import math
from collections.abc import Callable, Collection, Iterator, Sequence
from typing import Protocol

from data_pipeline import credentials as keys
from data_pipeline.store.errors import (
    CatalogError,
    KeyRejectedError,
    NetworkError,
    PeriodError,
    QuotaExhaustedError,
    RateLimitedError,
)
from data_pipeline.store.model import (
    CatalogEntry,
    Failure,
    FetchBatch,
    Kind,
    Outcome,
    Request,
    SeriesData,
)

MISSING = frozenset({"", ".", ":", "N/E", "NA", "NAN", "N/A", "-"})
UNSUPPORTED_FREQUENCY = "unsupported frequency"
DECLARE_FREQUENCY = "the source does not report a frequency: declare `frequency` in the catalog"


_CLOCK: contextvars.ContextVar[Callable[[], datetime.datetime] | None] = contextvars.ContextVar("clock", default=None)


def utc_today() -> datetime.date:
    """Today in UTC by the clock of the sync that is running (Store(clock=...)), else the system's."""
    clock = _CLOCK.get()
    return (clock() if clock is not None else datetime.datetime.now(datetime.UTC)).date()


@contextlib.contextmanager
def clock_of_sync(clock: Callable[[], datetime.datetime]) -> Iterator[None]:
    """While a sync runs, the sources' `utc_today` reads its clock rather than the system's."""
    token = _CLOCK.set(clock)
    try:
        yield
    finally:
        _CLOCK.reset(token)


class Source(Protocol):
    name: str
    kind: Kind
    requests_per_minute: int
    daily_budget: int | None  # None when the source has no daily quota

    def validate(self, entry: CatalogEntry) -> None:
        """Raise CatalogError when the entry carries a field this source does not accept."""
        ...

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        """Yield batches as they are downloaded. Raise QuotaExhaustedError to stop early."""
        ...


def number(value: object) -> float:
    """Source value -> float; the source's missing sentinels -> NaN (never 0)."""
    if value is None:
        return math.nan
    if isinstance(value, int | float):
        return float(value)
    text = str(value).strip()
    if text.upper() in MISSING:
        return math.nan
    return float(text.replace(",", ""))


def reject_params(entry: CatalogEntry, allowed: Collection[str] = ()) -> None:
    """Raise when the entry carries a source-specific field outside `allowed`."""
    unknown = sorted(set(entry.params) - set(allowed))
    if unknown:
        fields = ", ".join(unknown)
        msg = f"{entry.key}: unknown field(s) for source {entry.source!r}: {fields}"
        raise CatalogError(msg)


def failures(requests: Sequence[Request], outcome: Outcome, reason: str) -> tuple[Failure, ...]:
    return tuple(Failure(request.entry, outcome, reason) for request in requests)


def missing_key(requests: Sequence[Request], variable: str) -> FetchBatch:
    """The batch a source yields when its credential is absent: nothing is requested."""
    return FetchBatch(failures=failures(requests, Outcome.KEY_ERROR, keys.missing_message(variable)))


Download = Callable[[Request], SeriesData | Failure]


def per_request(requests: Sequence[Request], download: Download) -> Iterator[FetchBatch]:
    """Run `download` once per request and yield one batch per request."""
    for position, request in enumerate(requests):
        try:
            result = download(request)
        except KeyRejectedError as exc:
            yield FetchBatch(failures=failures(requests[position:], Outcome.KEY_ERROR, str(exc)))
            return
        except RateLimitedError as exc:
            # Asking again would only prolong the block: stop here, the next run resumes.
            raise QuotaExhaustedError(str(exc)) from exc
        except NetworkError as exc:
            yield FetchBatch(failures=(Failure(request.entry, Outcome.NETWORK_ERROR, str(exc)),))
            continue
        except (KeyError, IndexError, TypeError, ValueError, PeriodError) as exc:
            reason = f"unexpected answer ({type(exc).__name__}: {exc})"
            yield FetchBatch(failures=(Failure(request.entry, Outcome.SOURCE_ERROR, reason),))
            continue
        if isinstance(result, Failure):
            yield FetchBatch(failures=(result,))
        else:
            yield FetchBatch(series=(result,))
