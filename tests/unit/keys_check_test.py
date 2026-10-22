import datetime
import json

import httpx
import pytest

from data_pipeline import keys_check
from data_pipeline.keys_check import Verdict, check, check_batches
from data_pipeline.store.errors import QuotaExhaustedError
from data_pipeline.store.model import CatalogEntry, Failure, FetchBatch, Frequency, Outcome, SeriesData

from .store.helpers import fixture

TODAY = datetime.date(2026, 10, 6)
KEY = "0123456789abcdef0123456789abcdef"
ENTRY = CatalogEntry("fred", "UNRATE")


def batches(*items):
    yield from items


def raising(error):
    raise error
    yield  # pragma: no cover


def test_data_means_the_key_works():
    series = SeriesData(
        entry=ENTRY,
        key=ENTRY.key,
        name="x",
        frequency=Frequency.MONTHLY,
        units="",
        seasonal_adjustment="",
        observations=(),
    )
    result = check_batches("FRED_API_KEY", batches(FetchBatch(series=(series,))))
    assert result.verdict is Verdict.OK


def test_a_key_error_means_rejected_and_keeps_the_reason():
    failure = Failure(ENTRY, Outcome.KEY_ERROR, "FRED rejected the key: bad")
    result = check_batches("FRED_API_KEY", batches(FetchBatch(failures=(failure,))))
    assert (result.verdict, result.detail) == (Verdict.REJECTED, "FRED rejected the key: bad")


@pytest.mark.parametrize("outcome", [Outcome.NETWORK_ERROR, Outcome.SOURCE_ERROR, Outcome.NOT_FOUND])
def test_other_failures_mean_unknown(outcome):
    result = check_batches("FRED_API_KEY", batches(FetchBatch(failures=(Failure(ENTRY, outcome, "x"),))))
    assert result.verdict is Verdict.UNKNOWN


def test_a_quota_or_an_exception_means_unknown_and_never_raises():
    assert check_batches("FRED_API_KEY", raising(QuotaExhaustedError("quota"))).verdict is Verdict.UNKNOWN
    assert check_batches("FRED_API_KEY", raising(RuntimeError("boom"))).verdict is Verdict.UNKNOWN
    assert check_batches("FRED_API_KEY", batches()).verdict is Verdict.UNKNOWN


def fred(*, rejected=False, down=False):
    def handler(request):
        if down:
            msg = "offline"
            raise httpx.ConnectError(msg)
        if rejected:
            return httpx.Response(
                400, text='{"error_message":"Bad Request.  The value for variable api_key is not registered."}'
            )
        if request.url.path.endswith("/observations"):
            row = {"realtime_start": "2026-09-04", "date": "2026-08-01", "value": "4.3"}
            return httpx.Response(200, text=json.dumps({"observations": [row]}))
        meta = {
            "id": "UNRATE",
            "title": "Unemployment Rate",
            "frequency_short": "M",
            "units": "Percent",
            "seasonal_adjustment_short": "SA",
        }
        return httpx.Response(200, text=json.dumps({"seriess": [meta]}))

    return httpx.MockTransport(handler)


def test_fred_end_to_end():
    assert check("FRED_API_KEY", KEY, transport=fred(), today=TODAY).verdict is Verdict.OK
    rejected = check("FRED_API_KEY", KEY, transport=fred(rejected=True), today=TODAY)
    assert rejected.verdict is Verdict.REJECTED
    assert KEY not in rejected.detail
    assert check("FRED_API_KEY", KEY, transport=fred(down=True), today=TODAY).verdict is Verdict.UNKNOWN


def test_a_key_the_url_encodes_never_shows_in_the_detail_of_a_check():
    awkward = "abc+def/ghi=jk l"  # sent as abc%2Bdef%2Fghi%3Djk+l: the value as typed is not in the URL

    def echo(request):
        return httpx.Response(400, text=json.dumps({"error_message": f"Bad Request. {request.url.query.decode()}"}))

    rejected = check("FRED_API_KEY", awkward, transport=httpx.MockTransport(echo), today=TODAY)
    assert rejected.verdict is Verdict.REJECTED
    assert rejected.detail == "FRED rejected the key: " + json.dumps(
        {"error_message": "Bad Request. series_id=UNRATE&api_key=***&file_type=json"}
    )


def test_an_empty_value_is_rejected_without_a_request():
    assert check("FRED_API_KEY", "  ", transport=fred(down=True)).verdict is Verdict.REJECTED


def sec(status):
    return httpx.MockTransport(
        lambda request: httpx.Response(status, text='{"0": {"cik_str": 320193, "ticker": "AAPL"}}')
    )


def test_the_sec_user_agent():
    assert check("SEC_EDGAR_UA", "Ana Ana@example.com", transport=sec(200)).verdict is Verdict.OK
    assert check("SEC_EDGAR_UA", "Ana Ana@example.com", transport=sec(403)).verdict is Verdict.REJECTED
    no_contact = check("SEC_EDGAR_UA", "Ana", transport=sec(200))
    assert no_contact.verdict is Verdict.REJECTED
    assert "e-mail" in no_contact.detail


def test_every_registry_key_has_a_check():
    from data_pipeline import credentials

    assert set(keys_check.PROBED) | {credentials.SEC_UA, credentials.INEGI} == set(
        credentials.NAMES
    )


def test_lines_are_ascii():
    for verdict in Verdict:
        assert keys_check.Check("FRED_API_KEY", verdict, "detail").line().isascii()


def test_every_probe_is_a_valid_catalog_entry():
    from data_pipeline.credentials import Credentials
    from data_pipeline.store.http import Client
    from data_pipeline.store.sources import create

    with Client(sleep=lambda _s: None, retries=0) as client:
        for name, (source_name, entry) in keys_check._probes(TODAY).items():
            create(source_name, client, Credentials({name: KEY})).validate(entry)


def test_comtrade_probe_asks_for_one_call():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, text=json.dumps({"data": []}))

    today = datetime.datetime.now(datetime.UTC).date()
    result = check("COMTRADE_API_KEY", KEY, transport=httpx.MockTransport(handler), today=today)
    assert result.verdict is Verdict.OK
    assert len(seen) == 1
    assert KEY not in str(seen[0].url)


def sharadar(status=200, rows=1, seen=None):
    """A fake Sharadar that answers every call with `status` and `rows` invented price rows."""
    row = {
        "date": "2026-10-05",
        "open": 1.0,
        "high": 1.0,
        "low": 1.0,
        "close": 1.0,
        "volume": 10,
        "closeadj": 1.0,
        "closeunadj": 1.0,
    }

    def handler(request):
        if seen is not None:
            seen.append(request)
        return httpx.Response(status, json={"count": rows, "data": [row] * rows})

    return httpx.MockTransport(handler)


def test_sharadar_end_to_end():
    seen = []
    assert check("SHARADAR_API_KEY", KEY, transport=sharadar(seen=seen), today=TODAY).verdict is Verdict.OK
    (request,) = seen
    # a ticker outside the free tier, which answers a wrong key as if it were right; about a year of it
    assert (request.url.params["ticker"], request.url.params["date.gte"]) == ("GOOGL", "2025-09-01")
    rejected = check("SHARADAR_API_KEY", KEY, transport=sharadar(403), today=TODAY)
    assert rejected.verdict is Verdict.REJECTED
    assert "free tier" in rejected.detail
    assert KEY not in rejected.detail
    assert check("SHARADAR_API_KEY", KEY, transport=sharadar(rows=0), today=TODAY).verdict is Verdict.UNKNOWN


def test_a_sec_404_cannot_tell():
    result = check("SEC_EDGAR_UA", "Ana Ana@example.com", transport=sec(404))
    assert result.verdict is Verdict.UNKNOWN
    assert "404" in result.detail


def test_every_client_is_created_with_the_short_timeout(monkeypatch):
    from data_pipeline.store.http import Client

    created = []

    class Spy(Client):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            created.append(self)

    monkeypatch.setattr(keys_check, "Client", Spy)
    check("FRED_API_KEY", KEY, transport=fred(), today=TODAY)
    check("SEC_EDGAR_UA", "Ana Ana@example.com", transport=sec(200))
    assert len(created) == 2
    assert all(client._http.timeout == httpx.Timeout(keys_check.CHECK_TIMEOUT) for client in created)
    assert keys_check.CHECK_TIMEOUT < 90


def test_the_banxico_probe_declares_its_frequency_so_it_costs_one_request():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, text=fixture("banxico_data.json"))

    result = check("BANXICO_TOKEN", KEY, transport=httpx.MockTransport(handler), today=TODAY)
    assert result.verdict is Verdict.OK
    assert len(seen) == 1


def test_banxico_a_bad_token_is_rejected():
    transport = httpx.MockTransport(lambda request: httpx.Response(401, text=fixture("banxico_bad_token.json")))
    assert check("BANXICO_TOKEN", KEY, transport=transport, today=TODAY).verdict is Verdict.REJECTED


def test_inegi_cannot_be_checked_and_no_request_is_made():
    def handler(request):
        pytest.fail("INEGI must not be requested")

    result = check("INEGI_TOKEN", KEY, transport=httpx.MockTransport(handler), today=TODAY)
    assert result.verdict is Verdict.UNKNOWN
    assert "any token" in result.detail


def test_bls_a_refused_key_is_rejected():
    refusal = {
        "status": "REQUEST_NOT_PROCESSED",
        "message": ["The key provided by the User is invalid."],
        "Results": {},
    }
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=refusal))
    result = check("BLS_API_KEY", KEY, transport=transport, today=TODAY)
    assert result.verdict is Verdict.REJECTED
    assert KEY not in result.detail
