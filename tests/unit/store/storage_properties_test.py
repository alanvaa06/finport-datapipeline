"""The four invariants of the store, checked on generated sync histories."""

import datetime
import math

import pandas as pd
from hypothesis import given
from hypothesis import strategies as st

from data_pipeline.store.storage import OBS_DTYPES, TOLERANCE, append_changes, as_of, empty_observations, latest, typed

START = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
DAYS = {"2026-01": "2026-01-31", "2026-02": "2026-02-28", "2026-03": "2026-03-31", "2026-04": "2026-04-30"}
VALUES = st.one_of(st.just(math.nan), st.floats(min_value=-1e6, max_value=1e6, allow_nan=False))
BATCHES = st.lists(st.dictionaries(st.sampled_from(sorted(DAYS)), VALUES, min_size=1), min_size=1, max_size=6)


def moment(step):
    return START + datetime.timedelta(days=step)


def frame(batch, step):
    return typed(
        [
            {
                "key": "fred:X",
                "period": period,
                "date": DAYS[period],
                "value": value,
                "projection": False,
                "fetched_at": moment(step),
                "published_at": None,
            }
            for period, value in batch.items()
        ],
        OBS_DTYPES,
    )


def same(left, right):
    return (math.isnan(left) and math.isnan(right)) or math.isclose(left, right, rel_tol=TOLERANCE, abs_tol=0.0)


def replay(batches):
    """Apply the batches one sync after another. Returns the store after each sync."""
    stored = empty_observations()
    history = []
    for step, batch in enumerate(batches):
        stored, _, _ = append_changes(stored, frame(batch, step))
        history.append(stored)
    return history


@given(BATCHES)
def test_syncing_the_same_data_twice_adds_no_rows(batches):
    stored = replay(batches)[-1]
    again, added, revised = append_changes(stored, frame(batches[-1], len(batches)))
    assert (len(again), added, revised) == (len(stored), 0, 0)


@given(BATCHES)
def test_a_normal_read_returns_the_last_value_received(batches):
    expected = {}
    for batch in batches:
        expected.update(batch)
    current = latest(replay(batches)[-1])
    found = dict(zip(current["period"], current["value"], strict=True))
    assert found.keys() == expected.keys()
    assert all(same(found[period], expected[period]) for period in expected)


@given(BATCHES)
def test_an_as_of_read_never_returns_something_known_later(batches):
    history = replay(batches)
    final = history[-1]
    for step, then in enumerate(history):
        seen = as_of(final, pd.Timestamp(moment(step)))
        pd.testing.assert_frame_equal(seen, latest(then))


@given(BATCHES)
def test_no_stored_row_is_ever_changed_or_deleted(batches):
    history = replay(batches)
    final = history[-1]
    for step, then in enumerate(history):
        kept = final[final["fetched_at"] <= pd.Timestamp(moment(step))].reset_index(drop=True)
        pd.testing.assert_frame_equal(kept, then)
