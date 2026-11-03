"""Turn the period text a source sends into the store's own label and the day the period ends.

The store labels a day "2026-09-22", a month "2026-08", a quarter "2026Q2" and a year "2026".
Sources write the same periods many ways: a day that stands for its month or quarter, a Mexican
"day/month/year", "2026-M08", "2026-Q2", "2026/02". The frequency decides which spellings count.
"""

import calendar
import dataclasses
import datetime
import re

from data_pipeline.store.errors import PeriodError
from data_pipeline.store.model import Frequency

# A full date stands for whatever period holds it. The ISO spelling may carry a time after it.
_DATE_SPELLINGS = (
    re.compile(r"(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})"),
    re.compile(r"(?P<day>\d{2})/(?P<month>\d{2})/(?P<year>\d{4})\Z"),
)


@dataclasses.dataclass(frozen=True, slots=True)
class _Span:
    """A period that splits the year into equal runs of `months` months, numbered from 1."""

    what: str  # how an error names the period
    months: int
    spelling: re.Pattern[str]  # text that is not a date: `year` and, unless a year, `number`
    label: str  # format with `year` and `number`

    def per_year(self) -> int:
        return 12 // self.months


_SPANS = {
    Frequency.MONTHLY: _Span(
        "a month", 1, re.compile(r"(?P<year>\d{4})[-/]?M?(?P<number>\d\d?)"), "{year:04d}-{number:02d}"
    ),
    Frequency.QUARTERLY: _Span(
        "a quarter", 3, re.compile(r"(?P<year>\d{4})[-/]?Q?0?(?P<number>[1-4])"), "{year:04d}Q{number}"
    ),
    Frequency.ANNUAL: _Span("a year", 12, re.compile(r"(?P<year>\d{4})"), "{year:04d}"),
}

# One alternative per frequency, each group named after the frequency's value.
_SHAPE = re.compile(
    r"(?P<A>\d{4}\Z)"
    r"|(?P<Q>\d{4}-?Q[1-4]\Z)"
    r"|(?P<M>\d{4}-?M?\d{2}\Z)"
    r"|(?P<D>\d{4}-\d{2}-\d{2})"
)


def _calendar_date(text: str, original: str) -> datetime.date | None:
    """The date `text` spells, None when it is not spelled as a date at all."""
    for spelling in _DATE_SPELLINGS:
        found = spelling.match(text)
        if found is None:
            continue
        parts = {name: int(digits) for name, digits in found.groupdict().items()}
        try:
            return datetime.date(**parts)
        except ValueError as exc:
            msg = f"invalid date {original!r}"
            raise PeriodError(msg) from exc
    return None


def _year_and_number(span: _Span, text: str, original: str) -> tuple[int, int]:
    found = span.spelling.fullmatch(text)
    if found is None:
        msg = f"{original!r} is not {span.what}"
        raise PeriodError(msg)
    year = int(found["year"])
    number = int(found["number"]) if "number" in span.spelling.groupindex else 1
    if not 1 <= number <= span.per_year():
        msg = f"month out of range in {original!r}"
        raise PeriodError(msg)
    return year, number


def read_period(text: str, frequency: Frequency) -> tuple[str, datetime.date]:
    """Return (label, last day of the period) for a source's period text."""
    cleaned = text.strip()
    date = _calendar_date(cleaned, text)
    span = _SPANS.get(frequency)
    if span is None:  # daily and weekly periods are single days
        if date is None:
            msg = f"{text!r} is not a daily date"
            raise PeriodError(msg)
        return date.isoformat(), date
    if date is None:
        year, number = _year_and_number(span, cleaned, text)
    else:
        year, number = date.year, (date.month - 1) // span.months + 1
    closing_month = number * span.months
    last = datetime.date(year, closing_month, calendar.monthrange(year, closing_month)[1])
    return span.label.format(year=year, number=number), last


def infer_frequency(text: str) -> Frequency | None:
    """The frequency a period text is written in, for sources that spell it SDMX style.

    "2025" is annual, "2026-Q1" or "2026Q1" quarterly, "2026-05", "2026-M05" or "2026M05"
    monthly, "2026-09-23" daily. Anything else (a week such as "2026-W05") is None.
    """
    found = _SHAPE.match(text.strip())
    if found is None or found.lastgroup is None:
        return None
    return Frequency(found.lastgroup)
