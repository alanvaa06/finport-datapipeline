import datetime
import math

import pandas as pd
import pytest

from data_pipeline.store import storage as st
from data_pipeline.store.api import Store
from data_pipeline.store.errors import StoreError

# Every key, ticker and number here is invented.

T1 = datetime.datetime(2026, 3, 2, 9, 0, tzinfo=datetime.UTC)  # the first sync
T2 = datetime.datetime(2026, 4, 6, 9, 0, tzinfo=datetime.UTC)  # the second
P0 = datetime.datetime(2026, 1, 30, 23, 59, 59, 999999, tzinfo=datetime.UTC)  # days a source published
P1 = datetime.datetime(2026, 2, 13, 23, 59, 59, 999999, tzinfo=datetime.UTC)
P2 = datetime.datetime(2026, 2, 27, 23, 59, 59, 999999, tzinfo=datetime.UTC)
MOMENTS = [
    None,
    "2026-01-01",
    "2026-02-13",
    "2026-02-27",
    "2026-03-02T08:59:59Z",
    "2026-03-02T09:00:00Z",
    "2026-04-06",
    "2027-01-01",
]
PLAIN = st.TableSchema(("ticker", "period"), ("close",), ("note",))
VERSIONED = st.TableSchema(("concept", "end"), ("value",), ("form",), versioned=True)


def observation(key, period, day, value, fetched, published=None, projection=False):
    return {
        "key": key,
        "period": period,
        "date": day,
        "value": value,
        "projection": projection,
        "fetched_at": fetched,
        "published_at": published,
    }


SERIES = [
    observation("fake:AAA", "2026-01", "2026-01-31", 1.0, T1),
    observation("fake:AAA", "2026-01", "2026-01-31", 1.5, T2),  # revised
    observation("fake:AAA", "2026-02", "2026-02-28", 2.0, T1),
    observation("fake:AAA", "2026-02", "2026-02-28", math.nan, T2),  # revised to missing
    observation("fake:AAA", "2026-03", "2026-03-31", 3.0, T1, projection=True),
    observation("fake:AAA", "2026-03", "2026-03-31", 3.1, T2),
    observation("fake:AAA", "2026-Q1", "2026-03-31", 9.0, T1),  # the date of 2026-03: fetched first, read first
    observation("fake:AAA", "2026-04", "2026-04-30", 4.0, T2, P2),
    observation("fake:AAA", "2026-04", "2026-04-30", 4.4, T2),  # no publication: the earlier of one fetch
    observation("fake:BBB", "2026-01", "2026-01-31", 10.0, T1, P0),
    observation("fake:BBB", "2026-01", "2026-01-31", 11.0, T1, P1),  # a later vintage in the same fetch
    observation("fake:BBB", "2026-02", "2026-02-28", 20.0, T2, P2),
    observation("fake:BBB", "2026-02", "2026-02-28", 21.0, T2, P2),  # a tie on every stamp: the later row wins
    observation("fake:BBB", "2026-03", "2026-03-31", 30.0, T1, P2),
    observation("fake:BBB", "2026-03", "2026-03-31", 29.0, T2, P1),  # an older vintage, fetched later
]


def price(ticker, day, close, fetched, note=None):
    return st.table_frame(
        [{"ticker": ticker, "period": day, "date": day, "close": close, "note": note}], PLAIN, fetched
    )


def fact(concept, value, published, fetched, form="10-K"):
    row = {"concept": concept, "end": "2025-12-31", "date": "2025-12-31", "value": value, "form": form}
    return st.table_frame([{**row, "published_at": published}], VERSIONED, fetched)


def index_row(key, kind):
    source, source_id = key.split(":")
    return {"key": key, "source": source, "source_id": source_id, "name": f"Name of {source_id}", "kind": kind}


@pytest.fixture
def store(tmp_path):
    """A store with a series source, a table source and a versioned table source."""
    storage = st.Storage(tmp_path)
    storage.prepare()
    storage.write_observations("fake", st.typed(SERIES, st.OBS_DTYPES))
    plain = {
        "XAAA": pd.concat(
            [
                price("XAAA", "2026-03-03", 1.0, T1),
                price("XAAA", "2026-03-03", 1.1, T2),  # revised
                price("XAAA", "2026-03-02", 0.9, T2),  # an earlier day, stored later
            ]
        ).drop(columns="note"),  # written before `note` existed
        "XBBB": pd.concat(
            [
                price(None, "2026-03-01", 1.5, T1),  # a key column read as missing: sorted last
                price("XBBB", "2026-03-03", 2.0, T1),
                price("XBBB", "2026-03-03", 2.5, T1),  # a tie on every stamp: the later row wins
            ]
        ),
        "XC.C": price("XC.C", "2026-03-03", 3.0, T2, note="late"),  # an id with a dot, as BRK.B has
    }
    for name, frame in plain.items():
        storage.write_table("plain", name, frame.reset_index(drop=True), PLAIN)
    versioned = {
        "ACME": [
            fact("Revenues", 100.0, P1, T1),
            fact("Revenues", 90.0, P2, T1, form="10-K/A"),  # restated
            fact("Assets", 7.0, P1, T1),
            fact("Assets", 8.0, P1, T2),  # the same day of publication, fetched later
            fact("Debt", 5.0, None, T2, form=""),  # no day of publication: known from its fetch
        ],
        "BETA": [  # the same keys as ACME's: each id is a table of its own
            fact("Revenues", 50.0, P1, T1),
            fact("Revenues", 55.0, P1, T1, form="10-K/A"),  # a tie on every stamp
            fact("Revenues", 40.0, P0, T2),  # an older version, fetched later
        ],
    }
    for name, frames in versioned.items():
        storage.write_table("versioned", name, pd.concat(frames, ignore_index=True), VERSIONED)
    keys = [index_row("fake:AAA", "series"), index_row("fake:BBB", "series")]
    keys += [index_row(f"plain:{name}", "table") for name in plain]
    keys += [index_row(f"versioned:{name}", "table") for name in versioned]
    storage.write_index(st.typed(keys, st.INDEX_DTYPES))
    return Store(tmp_path)


def plain_values(frame):
    """The frame's values as Python objects, a missing one as None: DuckDB gives None for missing
    text where pandas has NaN, and microseconds where the store has nanoseconds."""
    values = frame.reset_index(drop=True).astype(object)
    return values.where(values.notna(), None)


def same(found, expected):
    pd.testing.assert_frame_equal(plain_values(found), plain_values(expected))


def joined(parts):
    """The frames one after another; pandas warns when it joins empty ones."""
    filled = [part for part in parts if not part.empty]
    return pd.concat(filled, ignore_index=True) if filled else parts[0]


# -- the rows of each source, as the store reads them ------------------------------------------


@pytest.mark.parametrize("moment", MOMENTS)
@pytest.mark.parametrize(
    ("key", "query"),
    [
        ("fake:AAA", "SELECT date, period, value FROM fake WHERE key = 'fake:AAA'"),
        ("fake:BBB", "SELECT date, period, value FROM fake WHERE key = 'fake:BBB'"),
    ],
)
def test_a_series_reads_as_store_series_reads_it(store, moment, key, query):
    same(store.sql(query, as_of=moment), store.series(key, as_of=moment))


@pytest.mark.parametrize("moment", MOMENTS)
def test_a_series_source_holds_every_key_in_the_order_of_store_series(store, moment):
    expected = joined([store.series(key, as_of=moment).assign(key=key) for key in ("fake:AAA", "fake:BBB")])
    same(store.sql("SELECT * FROM fake", as_of=moment), expected[["key", "date", "period", "value"]])


@pytest.mark.parametrize("moment", MOMENTS)
@pytest.mark.parametrize(("source", "names"), [("plain", ["XAAA", "XBBB", "XC.C"]), ("versioned", ["ACME", "BETA"])])
def test_a_table_source_reads_as_store_table_reads_it(store, moment, source, names):
    found = store.sql(f"SELECT * FROM {source}", as_of=moment)  # noqa: S608 - a name of the test
    parts = [store.table(source, name, as_of=moment) for name in names]
    expected = joined([part.assign(id=name) for name, part in zip(names, parts, strict=True)])
    same(found, expected[["id", *expected.columns[:-1]]])
    same(found.drop(columns="id"), store.table(source, as_of=moment))


def test_the_latest_rows_are_the_ones_the_store_reads(store):
    """The cases above, spelled out once: what each source reads today."""
    assert store.sql("SELECT period, value FROM fake WHERE key = 'fake:AAA'").to_dict("list") == {
        "period": ["2026-01", "2026-Q1", "2026-03", "2026-04"],
        "value": [1.5, 9.0, 3.1, 4.0],
    }
    assert store.sql("SELECT value FROM fake WHERE key = 'fake:BBB'")["value"].tolist() == [11.0, 21.0, 29.0]
    plain = store.sql("SELECT id, period, close, note FROM plain")
    assert plain["close"].tolist() == [0.9, 1.1, 2.5, 1.5, 3.0]
    assert plain["note"].tolist() == [None, None, None, None, "late"]
    versioned = store.sql("SELECT id, concept, value FROM versioned")
    assert list(zip(versioned["id"], versioned["concept"], versioned["value"], strict=True)) == [
        ("ACME", "Assets", 8.0),
        ("ACME", "Debt", 5.0),
        ("ACME", "Revenues", 90.0),
        ("BETA", "Revenues", 55.0),
    ]


# -- every stored row, and the index -----------------------------------------------------------


@pytest.mark.parametrize(
    ("key", "query"),
    [
        ("fake:AAA", "SELECT * EXCLUDE (key) FROM fake_versions WHERE key = 'fake:AAA'"),
        ("fake:BBB", "SELECT * EXCLUDE (key) FROM fake_versions WHERE key = 'fake:BBB'"),
    ],
)
def test_the_versions_of_a_series_are_its_revisions(store, key, query):
    same(store.sql(query), store.revisions(key))


@pytest.mark.parametrize(
    ("source", "name", "schema"),
    [("plain", "XAAA", PLAIN), ("plain", "XC.C", PLAIN), ("versioned", "BETA", VERSIONED)],
)
def test_the_versions_of_a_table_are_its_stored_rows(store, tmp_path, source, name, schema):
    found = store.sql(f"SELECT * FROM {source}_versions WHERE id = '{name}'")  # noqa: S608 - names of the test
    stored = st.Storage(tmp_path).read_table(source, name)
    expected = stored.reindex(columns=[*schema.columns, "fetched_at", "published_at"])
    same(found.drop(columns="id"), expected)
    assert set(found["id"]) == {name}
    assert str(found["fetched_at"].dt.tz) == "UTC"


def test_every_stored_row_is_a_version(store):
    counts = store.sql(
        "SELECT (SELECT count(*) FROM fake_versions) AS fake, (SELECT count(*) FROM plain_versions) AS plain, "
        "(SELECT count(*) FROM versioned_versions) AS versioned"
    )
    assert counts.to_dict("records") == [{"fake": 15, "plain": 7, "versioned": 8}]


def test_store_index_is_the_index(store):
    same(store.sql("SELECT * FROM store_index"), store.index())


def test_an_empty_store_has_only_the_index(tmp_path):
    (tmp_path / "tables" / "ghost").mkdir(parents=True)  # a source without files has no names
    store = Store(tmp_path)
    assert store.sql("SELECT count(*) AS rows FROM store_index")["rows"].tolist() == [0]
    with pytest.raises(StoreError, match=r"the names a query can read: store_index$"):
        store.sql("SELECT * FROM ghost")


# -- what sql refuses, and how it fails ---------------------------------------------------------


@pytest.mark.parametrize(
    ("query", "found"),
    [
        ("CREATE TABLE copy AS SELECT * FROM fake", "not CREATE"),
        ("INSERT INTO fake_versions SELECT * FROM fake_versions", "not INSERT"),
        ("COPY (SELECT * FROM fake) TO 'out.csv'", "not COPY"),
        ("ATTACH 'other.duckdb'", "not ATTACH"),
        ("INSTALL httpfs", "not LOAD"),
        ("SET threads = 1", "not SET"),
        ("SELECT 1; SELECT 2", "not 2 statements"),
        ("  ", "and the query is empty"),
    ],
)
def test_anything_but_one_select_is_refused_before_it_runs(store, tmp_path, monkeypatch, query, found):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(StoreError, match=f"^only one SELECT is accepted, {found}$"):
        store.sql(query)
    assert not (tmp_path / "out.csv").exists()
    assert not (tmp_path / "other.duckdb").exists()


def test_a_select_that_starts_with_with_is_accepted(store):
    query = "WITH latest AS (SELECT * FROM fake) SELECT count(*) AS rows FROM latest; "
    assert store.sql(query)["rows"].tolist() == [7]


def test_a_name_that_does_not_exist_fails_with_duckdbs_message_and_the_names_defined(store):
    with pytest.raises(StoreError) as caught:
        store.sql("SELECT * FROM prices")
    message = str(caught.value)
    assert "Table with name prices does not exist" in message
    assert message.splitlines()[-1] == (
        "the names a query can read: fake, fake_versions, plain, plain_versions, store_index, "
        "versioned, versioned_versions"
    )


def test_a_query_duckdb_cannot_parse_fails_with_its_message(store):
    with pytest.raises(StoreError, match=r"(?s)Parser Error: syntax error.*the names a query can read: fake"):
        store.sql("SELEC 1")


def test_as_of_takes_what_store_table_takes(store):
    with pytest.raises(StoreError, match="as_of '2026/06/15' is not a date"):
        store.sql("SELECT 1", as_of="2026/06/15")
    same(
        store.sql("SELECT * EXCLUDE (id) FROM plain", as_of=datetime.date(2026, 3, 2)),
        store.table("plain", as_of="2026-03-02"),
    )


def test_a_query_reads_in_utc_and_installs_nothing(store):
    settings = store.sql(
        "SELECT current_setting('TimeZone') AS zone, current_setting('autoinstall_known_extensions') AS install"
    )
    assert settings.to_dict("records") == [{"zone": "UTC", "install": False}]


def test_a_query_changes_no_file_of_the_store(store, tmp_path):
    def files():
        return {path: path.read_bytes() for path in sorted(tmp_path.rglob("*")) if path.is_file()}

    before = files()
    store.sql("SELECT * FROM plain_versions")
    with pytest.raises(StoreError):
        store.sql("COPY plain TO 'plain.csv'")
    assert files() == before
