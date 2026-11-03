"""The rules sync and fetch share for what a source answers, on invented data."""

import datetime

from data_pipeline.store.answers import Answers
from data_pipeline.store.errors import QuotaExhaustedError
from data_pipeline.store.model import DocumentData, Failure, FetchBatch, Outcome, TableData

from .helpers import client, entry, monthly

STOPPED = "the work stopped"
NOT_RETURNED = "nothing came for this id"


def answers(secrets=()):
    return Answers(client(secrets=secrets), stopped=STOPPED, not_returned=NOT_RETURNED)


def table(source_id, close):
    row = {"ticker": source_id, "period": "2026-01-02", "date": datetime.date(2026, 1, 2), "close": close}
    return TableData(
        entry=entry(source_id),
        key=f"fake:{source_id}",
        name=f"Table of {source_id}",
        rows=(row,),
        key_columns=("ticker", "period"),
        value_columns=("close",),
    )


def failure(source_id, reason="no such id", outcome=Outcome.NOT_FOUND):
    return Failure(entry(source_id), outcome, reason)


def test_the_first_series_of_a_key_counts_and_a_later_one_does_not():
    taken = answers()
    first = monthly(entry("AAA"), {"2026-01": 1.0})
    again = monthly(entry("AAA"), {"2026-01": 9.0})
    other = monthly(entry("BBB"), {"2026-01": 2.0})
    assert taken.take(FetchBatch(series=(first, again))).series == (first,)
    assert taken.take(FetchBatch(series=(again, other))).series == (other,)


def test_a_failure_after_the_series_of_its_key_does_not_count():
    taken = answers()
    series = monthly(entry("AAA"), {"2026-01": 1.0})
    batch = taken.take(FetchBatch(series=(series,), failures=(failure("AAA"),)))
    assert (batch.series, batch.failures) == ((series,), ())
    assert taken.take(FetchBatch(failures=(failure("AAA"),))).failures == ()


def test_a_series_after_the_failure_of_its_key_does_not_count():
    taken = answers()
    assert taken.take(FetchBatch(failures=(failure("AAA"),))).failures == (failure("AAA"),)
    assert taken.take(FetchBatch(series=(monthly(entry("AAA"), {"2026-01": 1.0}),))).series == ()


def test_only_the_first_failure_of_a_key_counts():
    taken = answers()
    first, second = failure("AAA", "first"), failure("AAA", "second")
    assert taken.take(FetchBatch(failures=(first, second))).failures == (first,)
    assert taken.take(FetchBatch(failures=(second,))).failures == ()


def test_every_table_counts_and_a_key_can_bring_rows_and_fail():
    taken = answers()
    one, two = table("AAA", 1.0), table("AAA", 2.0)
    batch = taken.take(FetchBatch(tables=(one,), failures=(failure("AAA"),)))
    assert (batch.tables, batch.failures) == ((one,), (failure("AAA"),))
    assert taken.take(FetchBatch(tables=(two,))).tables == (two,)


def test_every_document_counts():
    taken = answers()
    documents = DocumentData(entry=entry("AAA"), key="fake:AAA", name="Filings of AAA")
    assert taken.take(FetchBatch(documents=(documents, documents))).documents == (documents, documents)


def test_a_reason_is_scrubbed_of_the_clients_secrets():
    secret = "abcdef0123456789abcdef0123456789"  # noqa: S105 - an invented key
    taken = answers(secrets=(secret,))
    (kept,) = taken.take(FetchBatch(failures=(failure("AAA", f"refused api_key={secret}"),))).failures
    assert secret not in kept.reason
    assert (kept.entry, kept.outcome) == (entry("AAA"), Outcome.NOT_FOUND)


def test_a_key_never_answered_is_not_found():
    taken = answers()
    taken.take(FetchBatch(series=(monthly(entry("AAA"), {"2026-01": 1.0}),)))
    assert taken.unanswered([entry("AAA"), entry("BBB")]) == [Failure(entry("BBB"), Outcome.NOT_FOUND, NOT_RETURNED)]
    assert not taken.quota_exhausted


def test_a_key_that_brought_a_table_a_document_or_a_failure_was_answered():
    taken = answers()
    documents = DocumentData(entry=entry("CCC"), key="fake:CCC", name="Filings of CCC")
    taken.take(FetchBatch(tables=(table("AAA", 1.0),), failures=(failure("BBB"),), documents=(documents,)))
    wanted = [entry("AAA"), entry("BBB"), entry("CCC")]
    assert taken.unanswered(wanted) == []


def test_a_quota_leaves_the_keys_not_answered_to_the_quota():
    taken = answers()
    taken.take(FetchBatch(series=(monthly(entry("AAA"), {"2026-01": 1.0}),)))
    taken.stop(QuotaExhaustedError("quota used up"))
    assert taken.quota_exhausted
    reason = "the source's quota ran out before this id was fetched: quota used up"
    assert taken.unanswered([entry("AAA"), entry("BBB")]) == [Failure(entry("BBB"), Outcome.QUOTA_EXHAUSTED, reason)]


def test_a_quota_the_caller_counted_leaves_the_keys_not_answered_to_the_quota():
    taken = answers()
    taken.quota_exhausted = True  # a daily budget the caller keeps ran out, without an exception
    (left,) = taken.unanswered([entry("AAA")])
    assert left.outcome is Outcome.QUOTA_EXHAUSTED


def test_any_other_exception_fails_the_keys_not_answered_naming_its_type():
    taken = answers()
    taken.stop(RuntimeError("the answer made no sense"))
    assert not taken.quota_exhausted
    reason = f"{STOPPED}: RuntimeError: the answer made no sense"
    assert taken.unanswered([entry("AAA")]) == [Failure(entry("AAA"), Outcome.SOURCE_ERROR, reason)]


def test_the_reason_a_download_stopped_is_scrubbed():
    secret = "abcdef0123456789abcdef0123456789"  # noqa: S105 - an invented key
    taken = answers(secrets=(secret,))
    taken.stop(RuntimeError(f"GET ?api_key={secret} failed"))
    (left,) = taken.unanswered([entry("AAA")])
    assert secret not in left.reason
