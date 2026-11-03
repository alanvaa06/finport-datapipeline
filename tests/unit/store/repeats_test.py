import datetime

from data_pipeline.store.model import Frequency, Observation
from data_pipeline.store.sources.repeats import repeated_period

DAY = datetime.date(2026, 8, 31)
PUBLISHED = datetime.datetime(2026, 9, 1, tzinfo=datetime.UTC)


def test_one_period_twice_is_refused_with_its_label():
    observations = [
        Observation("2026-07", DAY, 1.0),
        Observation("2026-08", DAY, 2.0),
        Observation("2026-08", DAY, 3.0),
    ]
    reason = repeated_period(observations, Frequency.MONTHLY)
    assert reason == (
        "period 2026-08 comes more than once: the data is more frequent than the frequency M, or the answer repeats it"
    )


def test_distinct_periods_and_dated_versions_of_one_period_pass():
    assert (
        repeated_period([Observation("2026-07", DAY, 1.0), Observation("2026-08", DAY, 2.0)], Frequency.MONTHLY) is None
    )
    versions = [
        Observation("2026-08", DAY, 1.0, published_at=PUBLISHED),
        Observation("2026-08", DAY, 2.0, published_at=PUBLISHED + datetime.timedelta(days=30)),
    ]
    assert repeated_period(versions, Frequency.MONTHLY) is None
    assert repeated_period([*versions, versions[0]], Frequency.MONTHLY) is not None
