"""Period text of any source -> canonical period label + last day of the period.

Sources spell periods their own way (FRED "2026-08-01" for a month, Banxico "01/08/2026",
IMF "2026-M08", SDMX "2026-Q2"). The frequency says how to read the text.
"""

import calendar
import datetime
import re

from data_pipeline.store.errors import PeriodError
from data_pipeline.store.model import Frequency

_ISO_DAY = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")
_MX_DAY = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")
_MONTH = re.compile(r"^(\d{4})[-/]?M?(\d{1,2})$")
_QUARTER = re.compile(r"^(\d{4})[-/]?Q?0?([1-4])$")
_YEAR = re.compile(r"^(\d{4})$")
_MONTHS_IN_YEAR = 12


def _day(text: str) -> datetime.date | None:
    iso = _ISO_DAY.match(text)
    if iso:
        return datetime.date(int(iso[1]), int(iso[2]), int(iso[3]))
    mx = _MX_DAY.match(text)
    if mx:
        return datetime.date(int(mx[3]), int(mx[2]), int(mx[1]))
    return None


def _month_end(year: int, month: int) -> datetime.date:
    return datetime.date(year, month, calendar.monthrange(year, month)[1])


def read_period(text: str, frequency: Frequency) -> tuple[str, datetime.date]:
    """Return (label, last day of the period) for a source's period text."""
    stripped = text.strip()
    try:
        day = _day(stripped)
    except ValueError as exc:
        msg = f"invalid date {text!r}"
        raise PeriodError(msg) from exc
    if frequency in (Frequency.DAILY, Frequency.WEEKLY):
        if day is None:
            msg = f"{text!r} is not a daily date"
            raise PeriodError(msg)
        return day.isoformat(), day
    if frequency is Frequency.MONTHLY:
        if day is not None:
            year, month = day.year, day.month
        else:
            found = _MONTH.match(stripped)
            if found is None:
                msg = f"{text!r} is not a month"
                raise PeriodError(msg)
            year, month = int(found[1]), int(found[2])
        if not 1 <= month <= _MONTHS_IN_YEAR:
            msg = f"month out of range in {text!r}"
            raise PeriodError(msg)
        return f"{year:04d}-{month:02d}", _month_end(year, month)
    if frequency is Frequency.QUARTERLY:
        if day is not None:
            year, quarter = day.year, (day.month - 1) // 3 + 1
        else:
            found = _QUARTER.match(stripped)
            if found is None:
                msg = f"{text!r} is not a quarter"
                raise PeriodError(msg)
            year, quarter = int(found[1]), int(found[2])
        return f"{year:04d}Q{quarter}", _month_end(year, 3 * quarter)
    if day is not None:
        year = day.year
    else:
        found = _YEAR.match(stripped)
        if found is None:
            msg = f"{text!r} is not a year"
            raise PeriodError(msg)
        year = int(found[1])
    return f"{year:04d}", datetime.date(year, 12, 31)


_SHAPES: tuple[tuple[re.Pattern[str], Frequency], ...] = (
    (re.compile(r"^\d{4}$"), Frequency.ANNUAL),
    (re.compile(r"^\d{4}-?Q[1-4]$"), Frequency.QUARTERLY),
    (re.compile(r"^\d{4}-?M?\d{2}$"), Frequency.MONTHLY),
    (re.compile(r"^\d{4}-\d{2}-\d{2}"), Frequency.DAILY),
)


def infer_frequency(text: str) -> Frequency | None:
    """The frequency a period text is written in, for sources that spell it SDMX style.

    "2025" is annual, "2026-Q1" or "2026Q1" quarterly, "2026-05", "2026-M05" or "2026M05"
    monthly, "2026-09-23" daily. Anything else (a week such as "2026-W05") is None.
    """
    stripped = text.strip()
    for shape, frequency in _SHAPES:
        if shape.match(stripped):
            return frequency
    return None
