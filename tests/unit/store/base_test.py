import math

import pytest

from data_pipeline.store.errors import (
    CatalogError,
    KeyRejectedError,
    NetworkError,
    PeriodError,
    QuotaExhaustedError,
    RateLimitedError,
)
from data_pipeline.store.model import Failure, Outcome, Request
from data_pipeline.store.sources.base import missing_key, number, per_request, reject_params

from .helpers import entry, monthly


@pytest.mark.parametrize("text", ["", ".", "N/E", "na", "NaN", "n/a", "-", None])
def test_missing_sentinels_become_nan(text):
    assert math.isnan(number(text))


@pytest.mark.parametrize(("value", "expected"), [("4.1", 4.1), ("1,234.5", 1234.5), (3, 3.0), (" 2 ", 2.0)])
def test_numbers_are_parsed(value, expected):
    assert number(value) == expected


def test_reject_params_names_the_entry_and_the_field():
    with pytest.raises(CatalogError, match=r"fake:UNRATE.*reporters"):
        reject_params(entry(params={"reporters": ["MEX"]}))


def run(download, ids=("A", "B", "C")):
    requests = [Request(entry(source_id)) for source_id in ids]
    return list(per_request(requests, download))


def test_one_batch_per_request():
    batches = run(lambda request: monthly(request.entry, {"2026-05": 1.0}))
    assert [batch.series[0].key for batch in batches] == ["fake:A", "fake:B", "fake:C"]


def test_a_returned_failure_is_recorded_and_the_run_continues():
    def download(request):
        if request.entry.source_id == "B":
            return Failure(request.entry, Outcome.NOT_FOUND, "does not exist")
        return monthly(request.entry, {"2026-05": 1.0})

    batches = run(download)
    assert [len(batch.series) for batch in batches] == [1, 0, 1]
    assert batches[1].failures[0].outcome is Outcome.NOT_FOUND


def test_a_network_error_fails_only_that_request():
    def download(request):
        if request.entry.source_id == "A":
            msg = "fake: HTTP 503 after 4 attempts"
            raise NetworkError(msg)
        return monthly(request.entry, {"2026-05": 1.0})

    batches = run(download)
    assert batches[0].failures[0].outcome is Outcome.NETWORK_ERROR
    assert len(batches[1].series) == 1


def test_an_unexpected_answer_fails_only_that_request():
    def download(request):
        if request.entry.source_id == "A":
            return {}["observations"]
        return monthly(request.entry, {"2026-05": 1.0})

    batches = run(download)
    assert batches[0].failures[0].outcome is Outcome.SOURCE_ERROR
    assert "KeyError" in batches[0].failures[0].reason
    assert len(batches[2].series) == 1


def test_a_rejected_key_skips_the_rest_of_the_source():
    def download(request):
        if request.entry.source_id == "B":
            msg = "the key was rejected"
            raise KeyRejectedError(msg)
        return monthly(request.entry, {"2026-05": 1.0})

    batches = run(download)
    assert len(batches) == 2
    assert [failure.entry.key for failure in batches[1].failures] == ["fake:B", "fake:C"]
    assert {failure.outcome for failure in batches[1].failures} == {Outcome.KEY_ERROR}


def test_reject_params_lets_the_allowed_fields_through():
    reject_params(entry(params={"bank": "BISE"}), ("bank", "area"))
    with pytest.raises(CatalogError, match="for source 'fake': banco"):
        reject_params(entry(params={"bank": "BISE", "banco": "x"}), ("bank", "area"))


def test_missing_key_fails_every_request_and_names_the_variable():
    batch = missing_key([Request(entry("A")), Request(entry("B"))], "BLS_API_KEY")
    assert [failure.entry.key for failure in batch.failures] == ["fake:A", "fake:B"]
    assert {failure.reason for failure in batch.failures} == {
        "BLS_API_KEY is missing. Get one at https://data.bls.gov/registrationEngine/ and run: data-pipeline setup"
    }
    assert {failure.outcome for failure in batch.failures} == {Outcome.KEY_ERROR}


def test_a_period_the_source_cannot_read_fails_only_that_request():
    def download(request):
        if request.entry.source_id == "A":
            msg = "'2026-W05' is not a month"
            raise PeriodError(msg)
        return monthly(request.entry, {"2026-05": 1.0})

    batches = run(download)
    assert batches[0].failures[0].outcome is Outcome.SOURCE_ERROR
    assert "PeriodError" in batches[0].failures[0].reason
    assert len(batches[1].series) == 1


def test_a_colon_is_a_missing_value():
    assert math.isnan(number(":"))


def test_a_persistent_rate_limit_stops_the_source_as_an_exhausted_quota():
    def download(request):
        if request.entry.source_id == "B":
            msg = "fake: HTTP 429 after 4 attempts"
            raise RateLimitedError(msg)
        return monthly(request.entry, {"2026-05": 1.0})

    stream = per_request([Request(entry(source_id)) for source_id in ("A", "B", "C")], download)
    assert next(stream).series[0].key == "fake:A"
    with pytest.raises(QuotaExhaustedError, match="HTTP 429"):
        next(stream)
