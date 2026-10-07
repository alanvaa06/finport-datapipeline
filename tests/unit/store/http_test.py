import json
import logging

import httpx
import pytest

from data_pipeline.store.errors import NetworkError, RateLimitedError
from data_pipeline.store.http import Client, scrub


def test_returns_a_non_retryable_answer_as_it_is():
    waits = []
    client = Client(transport=httpx.MockTransport(lambda _request: httpx.Response(400, text="bad")), sleep=waits.append)
    response = client.get("fred", "https://example.test/x")
    assert response.status_code == 400
    assert waits == []
    assert client.calls == {"fred": 1}


def test_retries_then_succeeds():
    answers = iter([httpx.Response(503), httpx.Response(429), httpx.Response(200, text="ok")])
    waits = []
    client = Client(
        transport=httpx.MockTransport(lambda _request: next(answers)),
        sleep=waits.append,
        clock=lambda: 0.0,
    )
    response = client.get("fred", "https://example.test/x", per_minute=6_000_000)
    assert response.text == "ok"
    assert [wait for wait in waits if wait >= 2.0] == [2.0, 4.0]
    assert client.calls == {"fred": 3}
    assert client.throttled == {"fred": 1}


def test_gives_up_after_every_retry_with_a_scrubbed_message():
    def handler(_request):
        msg = "no route to host for key SECRET123"
        raise httpx.ConnectError(msg)

    client = Client(secrets=("SECRET123",), transport=httpx.MockTransport(handler), sleep=lambda _seconds: None)
    with pytest.raises(NetworkError, match="after 4 attempts") as raised:
        client.get("fred", "https://example.test/x")
    assert "SECRET123" not in str(raised.value)
    assert "***" in str(raised.value)


def test_waits_between_calls_to_the_same_source_only():
    now = [100.0]
    waits = []

    def sleep(seconds):
        waits.append(seconds)
        now[0] += seconds

    client = Client(
        transport=httpx.MockTransport(lambda _request: httpx.Response(200)),
        sleep=sleep,
        clock=lambda: now[0],
    )
    client.get("fred", "https://example.test/x", per_minute=60)
    client.get("fred", "https://example.test/x", per_minute=60)
    client.get("bls", "https://example.test/x", per_minute=60)
    assert waits == [1.0]


def test_scrub_hides_every_secret_and_ignores_empty_ones():
    assert scrub("key=abc&token=xyz", ["abc", "", "xyz"]) == "key=***&token=***"


def test_post_sends_a_json_body_and_counts_as_a_call():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json={"ok": True})

    client = Client(transport=httpx.MockTransport(handler), sleep=lambda _seconds: None)
    response = client.post("bls", "https://example.test/x", json={"seriesid": ["A"]})
    assert response.json() == {"ok": True}
    assert seen[0].method == "POST"
    assert json.loads(seen[0].content) == {"seriesid": ["A"]}
    assert client.calls == {"bls": 1}


def test_post_retries_and_scrubs_like_get():
    answers = iter([httpx.Response(503), httpx.Response(200, text="ok")])
    client = Client(transport=httpx.MockTransport(lambda _request: next(answers)), sleep=lambda _seconds: None)
    assert client.post("bls", "https://example.test/x", json={}, per_minute=6_000_000).text == "ok"
    assert client.calls == {"bls": 2}

    def refuse(_request):
        msg = "connection refused for key SECRET123"
        raise httpx.ConnectError(msg)

    failing = Client(secrets=("SECRET123",), transport=httpx.MockTransport(refuse), sleep=lambda _seconds: None)
    with pytest.raises(NetworkError, match="after 4 attempts") as raised:
        failing.post("bls", "https://example.test/x", json={"registrationkey": "SECRET123"})
    assert "SECRET123" not in str(raised.value)


def test_a_source_that_keeps_answering_429_raises_a_rate_limit_error():
    client = Client(transport=httpx.MockTransport(lambda _request: httpx.Response(429)), sleep=lambda _seconds: None)
    with pytest.raises(RateLimitedError, match="HTTP 429 after 4 attempts"):
        client.get("oecd", "https://example.test/x")
    assert issubclass(RateLimitedError, NetworkError)
    assert client.throttled == {"oecd": 4}


def test_the_key_never_shows_in_the_request_log_of_httpx(caplog):
    client = Client(secrets=("s3cr3t",), transport=httpx.MockTransport(lambda _request: httpx.Response(200)))
    with caplog.at_level(logging.INFO, logger="httpx"):
        client.get("fred", "https://example.test/x", params={"api_key": "s3cr3t"})
    client.close()
    assert "HTTP Request" in caplog.text
    assert "s3cr3t" not in caplog.text
    assert "api_key=***" in caplog.text
    with caplog.at_level(logging.INFO, logger="httpx"):
        logging.getLogger("httpx").info("after close: %s", "s3cr3t")
    assert "after close: s3cr3t" in caplog.text  # the filter leaves with the client
