"""What a source answered to a list of requests, by the rules sync and fetch share.

A source answers in batches. Per store key, the first series or failure counts: a failure after
the key's series is ignored, and so is a series after its failure. Every table and every document
counts, so a key can bring rows and fail. Reasons are scrubbed of the client's secrets. A
QuotaExhaustedError ends the download and leaves the keys not yet answered to the quota; any other
exception ends it with a source error that names the exception's type; a key the source never
answered is not found.

The caller drives the download and does what it wants with each batch (sync stores it, fetch
keeps it in memory): it passes every batch through `take`, every exception through `stop`, and
asks `unanswered` at the end.
"""

import dataclasses
from collections.abc import Iterable

from data_pipeline.store.errors import QuotaExhaustedError
from data_pipeline.store.http import Client
from data_pipeline.store.model import CatalogEntry, Failure, FetchBatch, Outcome

QUOTA_REACHED = "the source's quota ran out before this id was fetched"


def stopped_by(error: Exception, stopped: str) -> str:
    """Why a source that raised `error` stopped, its secrets not yet scrubbed: `stopped`, then the
    exception's type and message."""
    return f"{stopped}: {type(error).__name__}: {error}"


@dataclasses.dataclass
class Answers:
    """The keys a download answered so far. `stopped` begins the reason of every key left when
    the source broke; `not_returned` is the reason of a key it never answered. `quota_exhausted`
    is set by `stop`, or by a caller that counts its own budget."""

    client: Client
    stopped: str
    not_returned: str
    quota_exhausted: bool = False
    ended: str = ""  # why the download ended early, scrubbed; "" when it did not
    settled: set[str] = dataclasses.field(default_factory=set)  # keys whose series or failure counted
    answered: set[str] = dataclasses.field(default_factory=set)  # keys that brought anything

    def take(self, batch: FetchBatch) -> FetchBatch:
        """The part of `batch` that counts, its failures' reasons scrubbed."""
        series = []
        for item in batch.series:
            if item.key not in self.settled:
                self.settled.add(item.key)
                series.append(item)
        failures = []
        for failure in batch.failures:
            if failure.entry.key not in self.settled:
                self.settled.add(failure.entry.key)
                failures.append(dataclasses.replace(failure, reason=self.client.scrub(failure.reason)))
        self.answered.update(item.key for item in series)
        self.answered.update(failure.entry.key for failure in failures)
        self.answered.update(table.key for table in batch.tables)
        self.answered.update(data.key for data in batch.documents)
        return FetchBatch(tuple(series), tuple(failures), batch.tables, batch.documents)

    def stop(self, error: Exception) -> None:
        """End the download with `error`, raised by the source or while its batches were taken."""
        if isinstance(error, QuotaExhaustedError):
            self.quota_exhausted = True
            self.ended = self.client.scrub(f"{QUOTA_REACHED}: {error}")
        else:
            self.ended = self.client.scrub(stopped_by(error, self.stopped))

    def unanswered(self, entries: Iterable[CatalogEntry]) -> list[Failure]:
        """A failure for each of `entries` the download did not answer, in their order."""
        if self.quota_exhausted:
            outcome = Outcome.QUOTA_EXHAUSTED
        elif self.ended:
            outcome = Outcome.SOURCE_ERROR
        else:
            outcome = Outcome.NOT_FOUND
        reason = self.ended or ("" if self.quota_exhausted else self.not_returned)
        return [Failure(entry, outcome, reason) for entry in entries if entry.key not in self.answered]
