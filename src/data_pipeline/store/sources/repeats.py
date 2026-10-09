"""A series never holds one period twice.

Two observations with one period label (and, for a source that dates its versions, one
publication day) mean that the series is more frequent than the frequency it is read with, or
that the answer repeats a row. The store keeps one value per period and version, so storing them
would silently keep only one: the series fails instead.
"""

import datetime
from collections.abc import Iterable

from data_pipeline.store.model import Frequency, Observation


def repeated_period(observations: Iterable[Observation], frequency: Frequency) -> str | None:
    """Why these observations cannot be stored as they are, or None when no period repeats."""
    seen: set[tuple[str, datetime.datetime | None]] = set()
    for observation in observations:
        version = (observation.period, observation.published_at)
        if version in seen:
            return (
                f"period {observation.period} comes more than once: the data is more frequent than "
                f"the frequency {frequency.value}, or the answer repeats it"
            )
        seen.add(version)
    return None
