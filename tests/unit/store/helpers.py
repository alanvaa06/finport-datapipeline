import datetime
import pathlib
from collections.abc import Callable, Iterator, Mapping, Sequence

import httpx

from data_pipeline.store.errors import QuotaExhaustedError
from data_pipeline.store.http import Client
from data_pipeline.store.model import (
    CatalogEntry,
    Failure,
    FetchBatch,
    Frequency,
    Kind,
    Observation,
    Request,
    SeriesData,
)
from data_pipeline.store.periods import read_period
from data_pipeline.store.sources.base import reject_params

FIXTURES = pathlib.Path(__file__).parent / "fixtures"
NOW = datetime.datetime(2026, 6, 6, 12, 0, tzinfo=datetime.UTC)
Handler = Callable[[httpx.Request], httpx.Response]
# FRED's answer to a request for the vintages of a series that ALFRED does not keep
NOT_IN_ALFRED = {"error_message": "Bad Request.  The series does not exist in ALFRED but may exist in FRED."}


def fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def client(handler: Handler | None = None, secrets: tuple[str, ...] = ()) -> Client:
    """A client served by `handler`, with no real waits and no retries."""
    transport = httpx.MockTransport(handler) if handler else None
    return Client(secrets=secrets, transport=transport, sleep=lambda _seconds: None, retries=0)


def entry(source_id: str = "UNRATE", source: str = "fake", **fields) -> CatalogEntry:
    return CatalogEntry(source=source, source_id=source_id, **fields)


def monthly(catalog_entry: CatalogEntry, values: Mapping[str, float]) -> SeriesData:
    """A monthly series from {"2026-05": 4.1, ...}."""
    observations = tuple(
        Observation(*read_period(period, Frequency.MONTHLY), value) for period, value in values.items()
    )
    return SeriesData(
        entry=catalog_entry,
        key=catalog_entry.key,
        name=f"Name of {catalog_entry.source_id}",
        frequency=Frequency.MONTHLY,
        units="Percent",
        seasonal_adjustment="SA",
        observations=observations,
    )


class FakeSource:
    """A source that answers from a dictionary: source_id -> SeriesData or Failure.

    An id without an answer is not returned at all. `quota_after=n` raises QuotaExhaustedError
    before the request at position n. With a `client`, every request counts as one call.
    """

    kind = Kind.SERIES
    requests_per_minute = 6000

    def __init__(
        self,
        name: str = "fake",
        *,
        daily_budget: int | None = None,
        quota_after: int | None = None,
        client: Client | None = None,
    ) -> None:
        self.name = name
        self.daily_budget = daily_budget
        self.quota_after = quota_after
        self.answers: dict[str, SeriesData | Failure] = {}
        self.seen: list[Request] = []
        self._client = client

    def validate(self, entry: CatalogEntry) -> None:
        reject_params(entry)

    def fetch(self, requests: Sequence[Request]) -> Iterator[FetchBatch]:
        for position, request in enumerate(requests):
            if self.quota_after is not None and position >= self.quota_after:
                msg = "quota used up"
                raise QuotaExhaustedError(msg)
            self.seen.append(request)
            if self._client is not None:
                self._client.calls[self.name] = self._client.calls.get(self.name, 0) + 1
            answer = self.answers.get(request.entry.source_id)
            if answer is None:
                continue
            if isinstance(answer, Failure):
                yield FetchBatch(failures=(answer,))
            else:
                yield FetchBatch(series=(answer,))
