"""SQL over the store: one SELECT, answered by DuckDB from the store's own Parquet files.

    store.sql("SELECT ticker, period, close FROM sharadar_prices WHERE period = '2026-10-22'")
    store.sql("SELECT * FROM sec_xbrl WHERE concept = 'Revenues'", as_of="2024-03-01")

Each call opens a new DuckDB database in memory and, before the query runs, defines for every
source with stored data:

  <source>_versions  every stored row, read from the source's files, with `fetched_at` and
                     `published_at`
  <source>           the rows Store.series or Store.table returns: per key its latest version, or
                     the one in force at `as_of`
  store_index        the index, as Store.index() returns it

A series source gives `key` and then the columns of Store.series (`date`, `period`, `value`); a
table source gives `id`, the catalog id whose table holds the row, and then the columns of
Store.table. Its files are read together by column name, so a column a file was written without
reads as missing.

The views choose versions with window functions, by the rules of `storage.latest` and
`storage.as_of`, and sort their rows as those functions do (tests compare them row for row): per
key (a series: key and period; a table: its id and key columns) the row fetched last, and among
the rows of one fetch the one published last; for a versioned table, and as of a moment, the row
known last, known from its publication or else its fetch, and among those the one fetched last.
Rows that tie on all of it: the later one in its file. A series leaves out missing values and
projections, as Store.series does.

Only one statement runs, and only a SELECT (`WITH ... SELECT` too): DuckDB's parser reads the
query first, and anything else is refused before it runs. The database lives in memory and may
not install extensions, and the store's files are only read, so a query changes no file. It can
still read other files on the machine (`read_csv('...')`), with the rights of whoever runs it.
"""

import pathlib
from collections.abc import Sequence

import duckdb
import pandas as pd

from data_pipeline.store import storage as st
from data_pipeline.store.errors import StoreError

INDEX = "store_index"
VERSIONS = "_versions"
ONLY_SELECT = "only one SELECT is accepted"
# Times in UTC, as the store writes them; no extension downloaded or written to disk by a query
SETTINGS: dict[str, str | bool | int | float | list[str]] = {
    "TimeZone": "UTC",
    "autoinstall_known_extensions": False,
}
SERIES_COLUMNS = ["key", "period", "date", "value", "projection"]


def quote(name: str) -> str:
    """A name as a DuckDB identifier, quoted whatever it holds."""
    return '"' + name.replace('"', '""') + '"'


def _text(value: str) -> str:
    """Text as a DuckDB literal."""
    return "'" + value.replace("'", "''") + "'"


def answer(storage: st.Storage, query: str, moment: pd.Timestamp | None) -> pd.DataFrame:
    """The rows of `query`, a single SELECT, over the views of `storage` (see the module),
    each source read as of `moment` when one is given. StoreError for anything but one SELECT,
    before it runs, and for a query DuckDB refuses, with its message and the names defined."""
    series = storage.series_sources()
    tables = storage.table_sources()
    names = sorted([*series, *tables, *(source + VERSIONS for source in [*series, *tables]), INDEX])
    _check(query, names)
    with duckdb.connect(":memory:", config=SETTINGS) as connection:
        try:
            for source in series:
                _series_views(connection, storage, source, moment)
            for source in tables:
                _table_views(connection, storage, source, moment)
            connection.register(INDEX, storage.read_index())
            return connection.execute(query).df()
        except duckdb.Error as exc:
            raise _failed(exc, names) from exc


def _failed(error: duckdb.Error, names: Sequence[str]) -> StoreError:
    return StoreError(f"the query failed: {error}\nthe names a query can read: {', '.join(names)}")


def _check(query: str, names: Sequence[str]) -> None:
    """Refuse, before anything runs, a query that is not exactly one SELECT."""
    try:
        statements = duckdb.extract_statements(query)
    except duckdb.Error as exc:
        raise _failed(exc, names) from exc
    if not statements:
        msg = f"{ONLY_SELECT}, and the query is empty"
        raise StoreError(msg)
    if len(statements) > 1:
        msg = f"{ONLY_SELECT}, not {len(statements)} statements"
        raise StoreError(msg)
    kind = statements[0].type
    if kind != duckdb.StatementType.SELECT:
        msg = f"{ONLY_SELECT}, not {kind.name}"
        raise StoreError(msg)


def _rows(paths: Sequence[pathlib.Path]) -> str:
    """Every row of the Parquet files, read together by column name, with `id`, the name of its
    file, and `position`, where it sits in that file. The stamps are cast to TIMESTAMPTZ: DuckDB
    reads them as TIMESTAMPTZ_NS, which compares with no other time and reaches pandas without its
    zone (the store's stamps are whole microseconds, so nothing is lost)."""
    listed = ", ".join(_text(path.as_posix()) for path in paths)
    return (
        "(SELECT parse_filename(filename, true) AS id, "  # noqa: S608 - quoted names
        "* EXCLUDE (filename, file_row_number) "
        "REPLACE (fetched_at::TIMESTAMPTZ AS fetched_at, published_at::TIMESTAMPTZ AS published_at), "
        "file_row_number AS position "
        f"FROM read_parquet([{listed}], union_by_name = true, filename = true, file_row_number = true))"
    )


def _chosen(
    rows: str,
    columns: Sequence[str],
    key: Sequence[str],
    order: Sequence[str],
    moment: pd.Timestamp | None,
    *,
    by_publication: bool,
    keep: str = "true",
) -> str:
    """A SELECT of `columns` from `rows`: per `key` the row storage.latest chooses (or, given a
    moment, storage.as_of), among those that `keep`, sorted by `order` as those functions sort.

    `rows` holds `position`: of rows that tie on every stamp, pandas keeps the later one.
    """
    if moment is None and not by_publication:
        # the row fetched last, then published last; a missing stamp is the earliest
        rank = "fetched_at DESC NULLS LAST, published_at DESC NULLS LAST"
        stamps = "fetched_at NULLS FIRST, published_at NULLS FIRST"
        visible = "true"
    else:
        # the row known last, then fetched last; a missing stamp is the latest
        rank = "known_at DESC NULLS FIRST, fetched_at DESC NULLS FIRST"
        stamps = "known_at NULLS LAST, fetched_at NULLS LAST"
        visible = "true" if moment is None else f"known_at <= TIMESTAMPTZ {_text(moment.floor('us').isoformat())}"
    partition = ", ".join(quote(column) for column in key)
    sort = ", ".join(f"{quote(column)} NULLS LAST" for column in order)
    selected = ", ".join(quote(column) for column in columns)
    return (
        f"SELECT {selected} FROM ("  # noqa: S608 - quoted names
        f"SELECT *, row_number() OVER (PARTITION BY {partition} ORDER BY {rank}, position DESC) AS chosen "
        f"FROM (SELECT *, coalesce(published_at, fetched_at) AS known_at FROM {rows}) WHERE {visible}"
        f") WHERE chosen = 1 AND {keep} ORDER BY {sort}, {stamps}, position"
    )


def _series_views(
    connection: duckdb.DuckDBPyConnection, storage: st.Storage, source: str, moment: pd.Timestamp | None
) -> None:
    rows = _rows([storage.series_path(source)])
    every = ", ".join(quote(column) for column in [*SERIES_COLUMNS, *st.STAMP_COLUMNS])
    connection.execute(
        f"CREATE VIEW {quote(source + VERSIONS)} AS SELECT {every} FROM {rows} "  # noqa: S608 - quoted names
        'ORDER BY "key", "date", fetched_at NULLS FIRST, published_at NULLS FIRST, position'
    )
    current = _chosen(
        rows,
        ["key", "date", "period", "value"],
        st.KEY,
        st.SERIES_ORDER,
        moment,
        by_publication=False,
        keep="value IS NOT NULL AND NOT isnan(value) AND NOT projection",
    )
    connection.execute(f"CREATE VIEW {quote(source)} AS {current}")


def _table_views(
    connection: duckdb.DuckDBPyConnection, storage: st.Storage, source: str, moment: pd.Timestamp | None
) -> None:
    schema = storage.table_schema(source)
    rows = _rows([storage.table_path(source, name) for name in storage.table_names(source)])
    every = ", ".join(quote(column) for column in ["id", *schema.columns, *st.STAMP_COLUMNS])
    connection.execute(
        f"CREATE VIEW {quote(source + VERSIONS)} AS SELECT {every} "  # noqa: S608 - quoted names
        f"FROM {rows} ORDER BY id, position"
    )
    current = _chosen(
        rows,
        ["id", *schema.columns],
        ["id", *schema.key_columns],
        ["id", *schema.row_order],
        moment,
        by_publication=schema.versioned,
    )
    connection.execute(f"CREATE VIEW {quote(source)} AS {current}")
