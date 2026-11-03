import datetime

import pytest

from data_pipeline.store.errors import PeriodError
from data_pipeline.store.model import Frequency
from data_pipeline.store.periods import infer_frequency, read_period


@pytest.mark.parametrize(
    ("text", "frequency", "label", "day"),
    [
        ("2026-09-22", Frequency.DAILY, "2026-09-22", datetime.date(2026, 9, 22)),
        ("22/09/2026", Frequency.DAILY, "2026-09-22", datetime.date(2026, 9, 22)),
        ("2026-09-18", Frequency.WEEKLY, "2026-09-18", datetime.date(2026, 9, 18)),
        ("2026-08-01", Frequency.MONTHLY, "2026-08", datetime.date(2026, 8, 31)),
        ("2026-M02", Frequency.MONTHLY, "2026-02", datetime.date(2026, 2, 28)),
        ("2024/02", Frequency.MONTHLY, "2024-02", datetime.date(2024, 2, 29)),
        ("2026-04-01", Frequency.QUARTERLY, "2026Q2", datetime.date(2026, 6, 30)),
        ("2026-Q4", Frequency.QUARTERLY, "2026Q4", datetime.date(2026, 12, 31)),
        ("2026-01-01", Frequency.ANNUAL, "2026", datetime.date(2026, 12, 31)),
        ("2026", Frequency.ANNUAL, "2026", datetime.date(2026, 12, 31)),
    ],
)
def test_reads_each_spelling(text, frequency, label, day):
    assert read_period(text, frequency) == (label, day)


@pytest.mark.parametrize(
    ("text", "frequency"),
    [
        ("2026-08", Frequency.DAILY),
        ("2026-13", Frequency.MONTHLY),
        ("soon", Frequency.QUARTERLY),
        ("26", Frequency.ANNUAL),
        ("2026-02-31", Frequency.MONTHLY),
    ],
)
def test_rejects_text_that_does_not_match_the_frequency(text, frequency):
    with pytest.raises(PeriodError, match=text):
        read_period(text, frequency)


@pytest.mark.parametrize(
    ("text", "frequency"),
    [
        ("2025", Frequency.ANNUAL),
        ("2026-Q1", Frequency.QUARTERLY),
        ("2026Q1", Frequency.QUARTERLY),
        ("2026-05", Frequency.MONTHLY),
        ("2026-M05", Frequency.MONTHLY),
        ("2026M05", Frequency.MONTHLY),
        (" 2026-09-23 ", Frequency.DAILY),
        ("2026-09-23T00:00:00", Frequency.DAILY),
        ("2026-W05", None),
        ("2026-S1", None),
        ("soon", None),
        ("", None),
    ],
)
def test_the_frequency_is_inferred_from_how_a_period_is_spelled(text, frequency):
    assert infer_frequency(text) is frequency


@pytest.mark.parametrize("text", ["2025", "2026-Q1", "2026Q1", "2026-05", "2026-M05", "2026M05", "2026-09-23"])
def test_whatever_infer_frequency_accepts_read_period_can_read(text):
    label, day = read_period(text, infer_frequency(text))
    assert label
    assert day.year in (2025, 2026)
