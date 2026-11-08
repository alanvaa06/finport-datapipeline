"""The plain values the store passes around.

What the user asks for (`CatalogEntry`, `Request`), what a source answers with (`SeriesData`,
`TableData`, `DocumentData`, or a `Failure`), and the small vocabularies they share.
"""

import datetime
import enum
import string
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import assert_never


class Kind(enum.StrEnum):
    """The shape of what a catalog entry brings."""

    SERIES = "series"
    TABLE = "table"
    DOCUMENT = "document"


class Frequency(enum.StrEnum):
    """How often a series has a value; the value is the letter catalogs and the index use."""

    DAILY = "D"
    WEEKLY = "W"
    MONTHLY = "M"
    QUARTERLY = "Q"
    ANNUAL = "A"


class Outcome(enum.StrEnum):
    """How a call to a source ended."""

    OK = "ok"
    NOT_FOUND = "not_found"
    KEY_ERROR = "key_error"
    NETWORK_ERROR = "network_error"
    SOURCE_ERROR = "source_error"
    QUOTA_EXHAUSTED = "quota_exhausted"


def stale_after(frequency: Frequency, override: int | None = None) -> int:
    """Days after which a series is stale: the entry's own threshold, else the frequency's.

    Counted from the end of the last period held, each default is one period plus the usual
    publication lag.
    """
    if override is not None:
        return override
    match frequency:
        case Frequency.DAILY:
            return 10  # about a week and a half
        case Frequency.WEEKLY:
            return 28  # four weeks
        case Frequency.MONTHLY:
            return 124  # about four months
        case Frequency.QUARTERLY:
            return 183  # half a year
        case Frequency.ANNUAL:
            return 730  # two years
    assert_never(frequency)


@dataclass(frozen=True, slots=True)
class CatalogEntry:
    """A line of the catalog: one thing to bring from one source.

    `params` is for the source alone; the store never looks inside it.
    """

    source: str
    source_id: str
    alias: str | None = None
    start: datetime.date | None = None
    stale_after_days: int | None = None
    attrs: Mapping[str, str] = field(default_factory=dict)
    params: Mapping[str, object] = field(default_factory=dict)
    name: str | None = None  # shown in the index instead of the name the source gives
    frequency: Frequency | None = None  # a fallback for sources that do not say

    @property
    def key(self) -> str:
        """`source:source_id`, the entry's name across the store."""
        return ":".join((self.source, self.source_id))


@dataclass(frozen=True, slots=True)
class Request:
    """What a source is asked for: the entry, and enough of the store's copy to ask for less.

    Series use `since`: the first day wanted, or None for the whole history.

    Tables use `held` and `full`. Each item of `held` is a stored (frequency, period) followed by
    the stored values of the columns the source lists in `held_by` (see the Source protocol),
    raw or passed through the reader the source gives for that column. That is how Comtrade, for
    one, tells a period stored for one partner, flow and HS level from the same period stored for
    another. `full` asks the source to fetch everything again; it keeps `held` so it can still
    turn away rows the stored table would not accept.

    Documents use `groups`: the names of the documents the store has already.
    """

    entry: CatalogEntry
    since: datetime.date | None = None
    held: frozenset[tuple[str, ...]] = frozenset()
    groups: frozenset[str] = frozenset()
    full: bool = False


@dataclass(frozen=True, slots=True)
class Observation:
    """One value of a series."""

    period: str  # the store's label, such as "2026-09-22", "2026-08", "2026Q2" or "2026"
    date: datetime.date  # the day the period ends
    value: float  # NaN for a period the source lists with no value
    projection: bool = False
    published_at: datetime.datetime | None = None  # the source's publication time, when it gives one


@dataclass(frozen=True, slots=True)
class SeriesData:
    """A series as a source sent it."""

    entry: CatalogEntry
    key: str
    name: str
    frequency: Frequency
    units: str = ""
    seasonal_adjustment: str = ""
    country: str = ""
    observations: tuple[Observation, ...] = ()
    attrs: Mapping[str, str] = field(default_factory=dict)  # merged over the entry's attrs in the index


@dataclass(frozen=True, slots=True)
class TableData:
    """The rows one call brought for a table entry.

    A row maps column names to values: the key columns, the value columns (numbers, NaN when
    missing), the attribute columns (text kept as is, never compared) and `date`, the day its
    period ends. Rows with `frequency` and `period` columns come back in `Request.held` next time.

    With `versioned`, the source dates each row through its own `published_at`, so one key can
    arrive in more than one version in a single call.
    """

    entry: CatalogEntry
    key: str
    name: str
    rows: tuple[Mapping[str, object], ...]
    key_columns: tuple[str, ...]
    value_columns: tuple[str, ...]
    stale_after_days: int | None = None  # the source's default when the entry sets none
    attribute_columns: tuple[str, ...] = ()
    versioned: bool = False
    attrs: Mapping[str, str] = field(default_factory=dict)  # merged over the entry's attrs in the index


@dataclass(frozen=True, slots=True)
class Failure:
    """The answer for an entry the source could not deliver: how the call ended, and why."""

    entry: CatalogEntry
    outcome: Outcome
    reason: str


_NAME_CHARACTERS = frozenset(string.ascii_letters + string.digits + "._-")
_RESERVED_STEMS = frozenset({"CON", "PRN", "AUX", "NUL"})
_NUMBERED_DEVICES = ("COM", "LPT")


def _names_a_device(name: str) -> bool:
    """True for names Windows gives to a device whatever their extension: `nul.htm` is the null device."""
    stem = name.partition(".")[0].upper()
    if stem in _RESERVED_STEMS:
        return True
    prefix, number = stem[:3], stem[3:]
    return prefix in _NUMBERED_DEVICES and len(number) == 1 and number in "123456789"


def check_name(name: str) -> None:
    """Raise ValueError unless `name` is safe as one file or folder name of the store on any system.

    Safe means: not empty; only ASCII letters, digits, `.`, `_` and `-`, so no path separator
    and no drive; no final `.`, which rules out `.` and `..` and the names Windows would quietly
    shorten (`a.htm.` opens `a.htm`); and not a Windows device name such as `CON`, `com1` or
    `nul.htm`. So whatever a source calls a file, it stays where the store puts it.
    """
    unsafe = not name or not _NAME_CHARACTERS.issuperset(name) or name.endswith(".") or _names_a_device(name)
    if unsafe:
        msg = f"unsafe file name {name!r}"
        raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class DocumentFile:
    """A single published file of a document."""

    name: str
    content: bytes
    url: str
    role: str  # its part in the document, for example "primary" or "exhibit"
    tag: str = ""  # added to the document's label in the file's stored name (ex99-1); empty for the main file

    def __post_init__(self) -> None:
        check_name(self.name)
        if self.tag:
            check_name(self.tag)


@dataclass(frozen=True, slots=True)
class Document:
    """Files that come out together and stay as they are, a filing for instance.

    With a `label`, the files are stored under readable names in the entry's folder: the label for
    the main file, `<label>_<tag>` for the others, each with its own extension (see
    storage.readable_names). Without one, they are stored as published, in a folder per group."""

    group: str  # names the document inside its catalog entry
    date: datetime.date  # its publication day
    files: tuple[DocumentFile, ...]
    attributes: Mapping[str, str] = field(default_factory=dict)  # its columns in the list of documents
    label: str = ""  # the readable base name of its files, such as AAPL_10-K_2025-09-27

    def __post_init__(self) -> None:
        check_name(self.group)
        if self.label:
            check_name(self.label)


@dataclass(frozen=True, slots=True)
class DocumentData:
    """The documents one call brought for an entry; an empty tuple means it got through and found nothing new."""

    entry: CatalogEntry
    key: str
    name: str
    documents: tuple[Document, ...] = ()
    stale_after_days: int | None = None  # the source's default when the entry sets none
    attrs: Mapping[str, str] = field(default_factory=dict)  # merged over the entry's attrs in the index


@dataclass(frozen=True, slots=True)
class FetchBatch:
    """Everything one fetch produced, good and bad."""

    series: tuple[SeriesData, ...] = ()
    failures: tuple[Failure, ...] = ()
    tables: tuple[TableData, ...] = ()
    documents: tuple[DocumentData, ...] = ()
