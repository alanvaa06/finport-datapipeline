import json
import logging
import urllib.parse

import httpx
import pytest
from hypothesis import given
from hypothesis import strategies as st

from data_pipeline.store.errors import NetworkError, RateLimitedError
from data_pipeline.store.http import Client, retry_after, scrub

from .helpers import client as served

# A key that a URL cannot carry as it is. httpx 0.28 sends it as abc%2Bdef%2Fghi%3Djk+l in a query
# value (0.27 as ...jk%20l) and as abc+def/ghi=jk%20l in a path, and none of them holds the key.
AWKWARD = "abc+def/ghi=jk l"


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
    assert client.calls == {"fred": 2}  # the retry after the 429 is the same request
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


@pytest.mark.parametrize(
    "written",
    [
        AWKWARD,
        urllib.parse.quote(AWKWARD, safe=""),  # abc%2Bdef%2Fghi%3Djk%20l
        urllib.parse.quote_plus(AWKWARD),  # abc%2Bdef%2Fghi%3Djk+l
        str(httpx.QueryParams({"k": AWKWARD})).removeprefix("k="),  # what this httpx writes in a query
        "abc%2bdef%2fghi%3djk%20l",  # the hex digits in lower case, as .NET writes them
        "abc+def/ghi=jk%20l",  # only what a path cannot hold
        "abc%2Bdef/ghi=jk+l",  # any mix
    ],
    ids=["plain", "quote", "quote_plus", "httpx", "lower-hex", "path", "mixed"],
)
def test_scrub_hides_a_secret_as_a_url_encodes_it(written):
    url = f"https://example.test/x?api_key={written}&file_type=json"
    assert scrub(url, [AWKWARD]) == "https://example.test/x?api_key=***&file_type=json"


# Where a source puts its key, and the URL a message or a log line may then show.
IN_A_QUERY = ("https://example.test/x", {"api_key": AWKWARD}, "https://example.test/x?api_key=***")  # FRED
IN_A_PATH = (f"https://example.test/2.0/{AWKWARD}", {"type": "json"}, "https://example.test/2.0/***?type=json")  # INEGI
SENT = pytest.mark.parametrize(("url", "params", "shown"), [IN_A_QUERY, IN_A_PATH], ids=["query", "path"])


@SENT
def test_scrub_hides_a_secret_in_the_url_httpx_builds(url, params, shown):
    with httpx.Client() as http:
        built = str(http.build_request("GET", url, params=params).url)
    assert AWKWARD not in built  # httpx encoded it: looking for the key as it is finds nothing
    assert scrub(built, [AWKWARD]) == shown


@SENT
def test_a_key_the_url_encodes_never_shows_in_the_message_of_a_failed_request(url, params, shown):
    def handler(request):
        msg = f"no route to host for {request.url}"
        raise httpx.ConnectError(msg)

    with pytest.raises(NetworkError) as raised:
        served(handler, secrets=(AWKWARD,)).get("fred", url, params=params)
    assert str(raised.value) == f"fred: ConnectError: no route to host for {shown} after 1 attempt"


@SENT
def test_a_key_the_url_encodes_never_shows_in_the_request_log_of_httpx(caplog, url, params, shown):
    http = served(lambda _request: httpx.Response(200), secrets=(AWKWARD,))
    with caplog.at_level(logging.INFO, logger="httpx"):
        http.get("fred", url, params=params)
    http.close()
    logged = [record.getMessage() for record in caplog.records if record.name == "httpx"]
    assert logged == [f'HTTP Request: GET {shown} "HTTP/1.1 200 OK"']


def test_an_excerpt_hides_a_key_the_url_encodes_before_it_is_cut():
    text = "." * 280 + "api_key=" + urllib.parse.quote_plus(AWKWARD) + " and more"
    excerpt = served(secrets=(AWKWARD,)).excerpt(text, 300)
    assert excerpt == "." * 280 + "api_key=***" + " and more"


def test_scrub_hides_a_secret_that_is_not_ascii_as_its_utf8_bytes():
    assert scrub("token=cl%C3%A9%E2%82%AC&x=1 and clé€", ["clé€"]) == "token=***&x=1 and ***"


@given(st.text(min_size=1))
def test_scrub_hides_any_secret_however_a_url_encodes_it(secret):
    forms = [
        secret,
        urllib.parse.quote(secret, safe=""),
        urllib.parse.quote_plus(secret),
        str(httpx.QueryParams({"k": secret})).removeprefix("k="),
    ]
    assert [scrub(written, [secret]) for written in forms] == ["***"] * len(forms)


def test_scrub_hides_a_secret_whose_tilde_an_older_encoder_wrote_as_7e():
    # httpx and urllib leave "~" as it is; Java's URLEncoder and PHP's urlencode write %7E
    assert scrub("token=a%7Eb-c&x=1", ["a~b-c"]) == "token=***&x=1"


def test_scrub_hides_whole_a_secret_that_holds_a_percent_sign():
    # "%25" is the secret as it is and the start of the secret encoded: no "25" is left behind
    assert scrub("key=%2525&x=1 and %25", ["%25"]) == "key=***&x=1 and ***"


def test_scrub_never_raises_for_a_secret_that_is_not_text():
    # bytes of the environment that are not UTF-8 arrive as half surrogate pairs, which have no %XX
    assert scrub("key=\udcff&x=1", ["\udcff"]) == "key=***&x=1"


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


def test_an_excerpt_is_scrubbed_before_it_is_cut():
    key = "abcdef0123456789abcdef0123456789"
    text = "." * 280 + "api_key=" + key + " and more"
    client = Client(secrets=(key,))
    excerpt = client.excerpt(text, 300)
    assert len(excerpt) == 300
    assert key[:12] not in excerpt
    assert excerpt.endswith("api_key=***" + " and more"[: 300 - 291])


@pytest.mark.parametrize("name", ["httpcore.http11", "httpcore.connection", "httpcore.http2", "httpcore.proxy"])
def test_the_key_never_shows_in_the_debug_log_of_httpcore(caplog, name):
    # httpcore logs on child loggers; a filter on "httpcore" alone does not see their records.
    client = Client(secrets=("s3cr3t",))
    with caplog.at_level(logging.DEBUG):
        logging.getLogger(name).debug("receive_response_headers.complete Location=/next?api_key=%s", "s3cr3t")
    client.close()
    assert "api_key=***" in caplog.text
    assert "s3cr3t" not in caplog.text


def redirecting(location, status=302):
    """A source at api.example.test that sends every request to `location` once; `seen` records them."""
    seen = []

    def handler(request):
        seen.append(request)
        if request.url.host == "api.example.test" and request.url.path == "/start":
            return httpx.Response(status, headers={"Location": location})
        return httpx.Response(200, text="ok")

    return handler, seen


def test_a_redirect_within_the_same_host_over_https_is_followed():
    handler, seen = redirecting("https://api.example.test/moved?api_key=s3cr3t")
    client = Client(transport=httpx.MockTransport(handler), sleep=lambda _seconds: None)
    response = client.get("banxico", "https://api.example.test/start", headers={"Bmx-Token": "s3cr3t"})
    assert response.text == "ok"
    assert [request.url.path for request in seen] == ["/start", "/moved"]
    assert seen[1].headers["Bmx-Token"] == "s3cr3t"


def test_each_redirect_hop_waits_its_turn_like_any_other_call():
    handler, seen = redirecting("https://api.example.test/moved")
    waits = []
    client = Client(transport=httpx.MockTransport(handler), sleep=waits.append, clock=lambda: 0.0)
    client.get("banxico", "https://api.example.test/start", per_minute=60)
    assert [request.url.path for request in seen] == ["/start", "/moved"]
    assert waits == [1.0]  # 60 a minute: the hop goes a second after the first request
    assert client.calls == {"banxico": 2}


@pytest.mark.parametrize(
    ("location", "status"),
    [
        ("https://elsewhere.example/collect", 302),  # another host
        ("http://api.example.test/moved", 301),  # the same host without TLS
        ("https://elsewhere.example/collect", 307),  # a POST body would follow
    ],
)
def test_a_redirect_to_another_host_or_to_plain_http_is_refused_without_sending_anything(location, status):
    handler, seen = redirecting(location, status)
    client = Client(secrets=("s3cr3t",), transport=httpx.MockTransport(handler), sleep=lambda _seconds: None)
    with pytest.raises(NetworkError, match=r"redirect to \S+ refused") as raised:
        client.post("bls", "https://api.example.test/start", json={"registrationkey": "s3cr3t"})
    assert [request.url.host for request in seen] == ["api.example.test"]
    assert "s3cr3t" not in str(raised.value)
    assert client.calls == {"bls": 1}


def test_a_429_waits_as_long_as_retry_after_asks_and_its_retries_are_not_more_calls():
    answers = iter([httpx.Response(429, headers={"Retry-After": "5"}), httpx.Response(429), httpx.Response(200)])
    waits = []
    transport = httpx.MockTransport(lambda _request: next(answers))
    client = Client(transport=transport, sleep=waits.append, clock=lambda: 0.0)
    assert client.get("oecd", "https://example.test/x", per_minute=6_000_000).status_code == 200
    assert [wait for wait in waits if wait >= 2.0] == [5.0, 4.0]
    assert client.calls == {"oecd": 1}  # one request, asked again because the source said "later"
    assert client.throttled == {"oecd": 2}


@pytest.mark.parametrize("later", ["3600", "Wed, 21 Oct 2099 07:28:00 GMT"])
def test_a_429_that_asks_for_a_long_wait_stops_at_once(later):
    waits = []
    client = Client(
        transport=httpx.MockTransport(lambda _request: httpx.Response(429, headers={"Retry-After": later})),
        sleep=waits.append,
    )
    with pytest.raises(RateLimitedError, match=r"HTTP 429, asked to wait \d+ s"):
        client.get("oecd", "https://example.test/x")
    assert client.throttled == {"oecd": 1}
    assert [wait for wait in waits if wait >= 2.0] == []


def test_a_retry_after_date_in_the_past_retries_without_waiting():
    answers = iter([httpx.Response(503, headers={"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"}), httpx.Response(200)])
    waits = []
    transport = httpx.MockTransport(lambda _request: next(answers))
    client = Client(transport=transport, sleep=waits.append, clock=lambda: 0.0)
    assert client.get("ecb", "https://example.test/x", per_minute=6_000_000).status_code == 200
    assert [wait for wait in waits if wait >= 1.0] == []


def test_after_three_failed_requests_a_source_is_not_called_again_in_this_run():
    seen = []

    def handler(request):
        seen.append(request.url.host)
        if request.url.host == "down.test":
            msg = "timed out"
            raise httpx.ReadTimeout(msg, request=request)
        return httpx.Response(200)

    client = Client(transport=httpx.MockTransport(handler), sleep=lambda _seconds: None)
    for _ in range(3):
        with pytest.raises(NetworkError, match="after 4 attempts"):
            client.get("dbnomics", "https://down.test/x")
    with pytest.raises(NetworkError, match=r"not asked: its last 3 requests failed \(ReadTimeout: timed out"):
        client.get("dbnomics", "https://down.test/x")
    assert seen == ["down.test"] * 12
    assert client.calls == {"dbnomics": 12}
    assert client.get("fred", "https://up.test/x").status_code == 200  # another source is still asked


def test_an_answer_resets_the_count_of_failed_requests():
    answers = iter([503, 503, 200, 503, 503, 503])
    client = Client(
        transport=httpx.MockTransport(lambda _request: httpx.Response(next(answers))),
        sleep=lambda _seconds: None,
        retries=0,
    )
    outcomes = []
    for _ in range(6):
        try:
            outcomes.append(client.get("ecb", "https://example.test/x").status_code)
        except NetworkError:
            outcomes.append("failed")
    assert outcomes == ["failed", "failed", 200, "failed", "failed", "failed"]
    assert client.calls == {"ecb": 6}


@pytest.mark.parametrize(
    ("value", "expected"),
    [("120", 120.0), (" 7 ", 7.0), ("²", None), ("٣", None), ("1.5", None), ("-1", None), ("", None)],
)
def test_retry_after_in_seconds_is_ascii_digits_only(value, expected):
    # "²" (superscript two) passes str.isdigit() and reaches httpx as latin-1; float() refuses it
    response = httpx.Response(503, headers=[(b"Retry-After", value.encode("utf-8"))])
    assert retry_after(response) == expected


def test_a_retry_after_that_is_not_a_number_falls_back_to_the_usual_wait():
    superscript = "²".encode("latin-1")
    answers = iter([httpx.Response(503, headers=[(b"Retry-After", superscript)]), httpx.Response(200)])
    waits = []
    transport = httpx.MockTransport(lambda _request: next(answers))
    client = Client(transport=transport, sleep=waits.append, clock=lambda: 0.0)
    assert client.get("ecb", "https://example.test/x", per_minute=6_000_000).status_code == 200
    assert [wait for wait in waits if wait >= 1.0] == [2.0]
