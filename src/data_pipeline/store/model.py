"""Types shared by the store: catalog entries, requests, downloaded series, failures."""

import dataclasses
import datetime
import enum
import re
from collections.abc import Mapping


class Kind(enum.StrEnum):
    SERIES = "series"
    TABLE = "table"
    DOCUMENT = "document"


class Frequency(enum.StrEnum):
    DAILY = "D"
    WEEKLY = "W"
    MONTHLY = "M"
    QUARTERLY = "Q"
    ANNUAL = "A"


class Outcome(enum.StrEnum):
    OK = "ok"
    NOT_FOUND = "not_found"
    KEY_ERROR = "key_error"
    NETWORK_ERROR = "network_error"
    SOURCE_ERROR = "source_error"
    QUOTA_EXHAUSTED = "quota_exhausted"


# A series is stale when its last real period ended more than this many days ago.
STALE_AFTER_DAYS: Mapping[Frequency, int] = {
    Frequency.DAILY: 10,
    Frequency.WEEKLY: 28,
    Frequency.MONTHLY: 124,
    Frequency.QUARTERLY: 183,
    Frequency.ANNUAL: 730,
}


def stale_after(frequency: Frequency, override: int | None = None) -> int:
    """Days after which a series is stale: the entry's own threshold, else the frequency's."""
    return override if override is not None else STALE_AFTER_DAYS[frequency]


@dataclasses.dataclass(frozen=True, slots=True)
class CatalogEntry:
    """One series the user wants. `params` holds the fields only its source understands."""

    source: str
    source_id: str
    alias: str | None = None
    start: datetime.date | None = None
    stale_after_days: int | None = None
    attrs: Mapping[str, str] = dataclasses.field(default_factory=dict)
    params: Mapping[str, object] = dataclasses.field(default_factory=dict)
    name: str | None = None  # replaces the source's own name in the index
    frequency: Frequency | None = None  # used only when the source does not report one

    @property
    def key(self) -> str:
        return f"{self.source}:{self.source_id}"


@dataclasses.dataclass(frozen=True, slots=True)
class Request:
    """A catalog entry plus what the store already has of it.

    For a series: `since`, the first date to ask for (`None` means full history).
    For a table: `held`, the (frequency, period) pairs already stored, each followed by its values
    in the columns the source names in its `held_by` (Source protocol), each as stored or as a
    function the source pairs with it reads it (Comtrade: partner, flow, and the HS level of the
    product code), so that a source can tell what one partner holds from what another does; and
    `full`, true when the sync is full: the source asks for everything again, and still has
    `held` to refuse what the stored table cannot take.
    For documents: `groups`, the names of the documents already stored.
    """

    entry: CatalogEntry
    since: datetime.date | None = None
    held: frozenset[tuple[str, ...]] = frozenset()
    groups: frozenset[str] = frozenset()
    full: bool = False


@dataclasses.dataclass(frozen=True, slots=True)
class Observation:
    period: str  # canonical label: "2026-09-22", "2026-08", "2026Q2", "2026"
    date: datetime.date  # last day of the period
    value: float  # NaN when the source lists the period without a value
    projection: bool = False
    published_at: datetime.datetime | None = None  # when the source published it, if it says so


@dataclasses.dataclass(frozen=True, slots=True)
class SeriesData:
    entry: CatalogEntry
    key: str
    name: str
    frequency: Frequency
    units: str = ""
    seasonal_adjustment: str = ""
    country: str = ""
    observations: tuple[Observation, ...] = ()
    attrs: Mapping[str, str] = dataclasses.field(default_factory=dict)  # added to the entry's in the index


@dataclasses.dataclass(frozen=True, slots=True)
class TableData:
    """Rows of one table of one catalog entry, as one call brought them.

    Every row is a mapping with the key columns, the value columns, the attribute columns and
    `date` (the last day of the period). Values are numbers; NaN is a missing value. Attribute
    columns are text that is stored but never compared. A table whose rows have `frequency` and
    `period` gets them back as `Request.held` on the next run.

    `versioned` means the source knows when each row was published: every row carries its own
    `published_at`, and a key may come with several versions at once.
    """

    entry: CatalogEntry
    key: str
    name: str
    rows: tuple[Mapping[str, object], ...]
    key_columns: tuple[str, ...]
    value_columns: tuple[str, ...]
    stale_after_days: int | None = None  # the source's own threshold, used when the entry has none
    attribute_columns: tuple[str, ...] = ()
    versioned: bool = False
    attrs: Mapping[str, str] = dataclasses.field(default_factory=dict)  # added to the entry's in the index


SAFE_NAME = re.compile(r"[A-Za-z0-9._-]+")
# Names Windows keeps for devices, with any extension (NUL.htm is the null device there)
DEVICE_NAMES = frozenset({"CON", "PRN", "AUX", "NUL", *(f"{port}{n}" for port in ("COM", "LPT") for n in range(1, 10))})


def check_name(name: str) -> None:
    """Raise ValueError unless `name` can be a file or folder name inside the store on every
    system: letters, digits, `.`, `_` and `-` only (so no separator, `/` or `\\`, and no drive),
    not ending in `.` (which also refuses `.` and `..`; Windows drops a final dot, so `a.htm.`
    would be `a.htm`), and not a name Windows keeps for a device (`CON`, `nul.htm`, `COM1`). A
    source's name never chooses another place."""
    if not SAFE_NAME.fullmatch(name) or name.endswith(".") or name.split(".")[0].upper() in DEVICE_NAMES:
        msg = f"unsafe file name {name!r}"
        raise ValueError(msg)


@dataclasses.dataclass(frozen=True, slots=True)
class DocumentFile:
    """One file of a document, as the source publishes it."""

    name: str
    content: bytes
    url: str
    role: str  # what the file is within its document, such as "primary" or "exhibit"

    def __post_init__(self) -> None:
        check_name(self.name)


@dataclasses.dataclass(frozen=True, slots=True)
class Document:
    """A group of files published together and never changed, such as one filing."""

    group: str  # the name of the document within its catalog entry
    date: datetime.date  # the day it was published
    files: tuple[DocumentFile, ...]
    attributes: Mapping[str, str] = dataclasses.field(default_factory=dict)  # columns of the list

    def __post_init__(self) -> None:
        check_name(self.group)


@dataclasses.dataclass(frozen=True, slots=True)
class DocumentData:
    """Documents of one catalog entry, as one call brought them. No documents means the entry
    was reached and had nothing new."""

    entry: CatalogEntry
    key: str
    name: str
    documents: tuple[Document, ...] = ()
    stale_after_days: int | None = None  # the source's own threshold, used when the entry has none
    attrs: Mapping[str, str] = dataclasses.field(default_factory=dict)  # added to the entry's in the index


@dataclasses.dataclass(frozen=True, slots=True)
class Failure:
    entry: CatalogEntry
    outcome: Outcome
    reason: str


@dataclasses.dataclass(frozen=True, slots=True)
class FetchBatch:
    series: tuple[SeriesData, ...] = ()
    failures: tuple[Failure, ...] = ()
    tables: tuple[TableData, ...] = ()
    documents: tuple[DocumentData, ...] = ()
