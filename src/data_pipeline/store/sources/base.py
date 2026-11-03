"""Pieces every source uses: the `Source` protocol, reading numbers, failure helpers and the
loop that downloads one request at a time.

How failures travel: a series the source does not know is recorded and the run moves on; a
rejected key ends that source's run; a network error or an answer that cannot be read costs only
the request it hit; a quota that is used up stops the source. No value is ever invented.
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

# Texts the sources use for "no value"; compared after strip() and upper().
MISSING = frozenset({"", ".", ":", "N/E", "NA", "NAN", "N/A", "-"})
UNSUPPORTED_FREQUENCY = "unsupported frequency"
DECLARE_FREQUENCY = "the source does not report a frequency: declare `frequency` in the catalog"

# The columns a table source tells its stored rows apart by (see Request.held). A plain name is
# sent as stored; a (name, read) pair sends read(stored value) instead.
HeldBy = tuple[str | tuple[str, Callable[[str], str]], ...]

Download = Callable[[Request], SeriesData | Failure]
_Requests = Sequence[Request]

_Now = Callable[[], datetime.datetime]
_sync_now: contextvars.ContextVar[_Now | None] = contextvars.ContextVar("sync_now", default=None)

# Errors that mean the answer did not have the shape the source expected.
_UNREADABLE = (KeyError, IndexError, TypeError, ValueError, PeriodError)


def utc_today() -> datetime.date:
    """The current UTC date, read from the running sync's clock if there is one."""
    now = _sync_now.get()
    if now is None:
        return datetime.datetime.now(datetime.UTC).date()
    return now().date()


@contextlib.contextmanager
def clock_of_sync(clock: _Now) -> Iterator[None]:
    """Make `utc_today` follow `clock` (the Store's) for as long as the block runs."""
    with contextlib.ExitStack() as restore:
        restore.callback(_sync_now.reset, _sync_now.set(clock))
        yield


class Source(Protocol):
    """What the store needs from a source."""

    name: str
    kind: Kind
    # For a table source, the columns that tell its stored rows apart (Request.held); () otherwise.
    held_by: HeldBy
    requests_per_minute: int
    daily_budget: int | None  # None: no daily quota

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        """Download `requests`, yielding each batch when it arrives; QuotaExhaustedError stops early."""
        ...

    def validate(self, entry: CatalogEntry) -> None:
        """Raise CatalogError if `entry` has a field this source does not take."""
        ...


def number(value: object) -> float:
    """A value from a source as a float. None and the MISSING texts give NaN, never 0."""
    match value:
        case None:
            return math.nan
        case int() | float():
            return float(value)
    cleaned = str(value).strip()
    # A comma is a thousands separator: "1,234.5" is 1234.5.
    return math.nan if cleaned.upper() in MISSING else float("".join(cleaned.split(",")))


def reject_params(entry: CatalogEntry, allowed: Collection[str] = ()) -> None:
    """Raise CatalogError naming every source-specific field of `entry` not in `allowed`."""
    permitted = frozenset(allowed)
    extra = [field for field in sorted(entry.params) if field not in permitted]
    if extra:
        msg = f"{entry.key}: unknown field(s) for source {entry.source!r}: {', '.join(extra)}"
        raise CatalogError(msg)


def failures(requests: _Requests, outcome: Outcome, reason: str) -> tuple[Failure, ...]:
    """One failure per request, all with the same outcome and reason."""
    entries = [request.entry for request in requests]
    return tuple(Failure(entry, outcome, reason) for entry in entries)


def missing_key(requests: _Requests, variable: str) -> FetchBatch:
    """Fail every request because the credential in `variable` is not set; nothing is asked."""
    why = keys.missing_message(variable)
    return FetchBatch(failures=failures(requests, Outcome.KEY_ERROR, why))


def per_request(requests: Sequence[Request], download: Download) -> Iterator[FetchBatch]:
    """Call `download` for each request in order, one batch per request.

    A rejected key fails the request it hit and all those after it, in one last batch.
    """
    for index, request in enumerate(requests):
        try:
            batch = _download_one(request, download)
        except KeyRejectedError as exc:
            yield FetchBatch(failures=failures(requests[index:], Outcome.KEY_ERROR, str(exc)))
            return
        yield batch


def _download_one(request: Request, download: Download) -> FetchBatch:
    """The batch of one request. A KeyRejectedError passes through; a lasting 429 ends the source."""
    try:
        got = download(request)
    except RateLimitedError as exc:
        # More requests would only stretch the block: stop the source, the next run picks up.
        raise QuotaExhaustedError(str(exc)) from exc
    except NetworkError as exc:
        got = Failure(request.entry, Outcome.NETWORK_ERROR, str(exc))
    except _UNREADABLE as exc:
        got = Failure(request.entry, Outcome.SOURCE_ERROR, f"unexpected answer ({type(exc).__name__}: {exc})")
    if isinstance(got, Failure):
        return FetchBatch(failures=(got,))
    return FetchBatch(series=(got,))
