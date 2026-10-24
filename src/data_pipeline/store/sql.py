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
reads as missing, and so does a column of the schema that no file has. Both views of a source read
a hidden view of its rows, `__rows_<source>`, whose working columns start with two underscores.

The views choose versions with window functions, by the rules of `storage.latest` and
`storage.as_of`, and sort their rows as those functions do (tests compare them row for row): per
key (a series: key and period; a table: its id and key columns) the row fetched last, and among
the rows of one fetch the one published last; for a versioned table, and as of a moment, the row
known last, known from its publication or else its fetch, and among those the one fetched last.
Rows that tie on all of it: the later one in its file. A series leaves out missing values and
projections, as Store.series does.

What a query may do, and what it may not:

  - One statement, and only one DuckDB's parser types as a SELECT (`WITH ... SELECT` too); any
    other kind is refused before anything runs. The parser also calls a `WITH` that holds a
    COPY, INSERT, UPDATE or DELETE a SELECT, so the statement runs as `SELECT * FROM (...)`, where
    DuckDB refuses a data-modifying statement. Two table functions run SQL from a text, at the
    top level, where that check does not reach: json_execute_serialized_sql() and query(). A call
    to either is refused, found in the tokens of the statement (query() must go too, as a text
    can build the other's name). The other functions of DuckDB 2.0 were read one by one and none
    else runs a statement from text; `import_database` and `copy_database` are not callable from a
    SELECT, and the settings and log functions are stopped by the locked configuration.
  - The database is in memory. Extensions are neither installed nor loaded by themselves (community
    ones are not allowed), time is read in UTC, spilled data goes to a folder of its own, and the
    configuration is locked before the query runs, so a function that changes a setting (a log
    file, say) is refused. The store's files are only read.
  - A SELECT can still read other files on the machine (`read_csv('...')`, `glob(...)`), with the
    rights of whoever runs it. Nothing here is a sandbox for a query you do not trust.
"""

import pathlib
import tempfile
from collections.abc import Sequence
from contextlib import suppress

import duckdb
import pandas as pd

from data_pipeline.store import storage as st
from data_pipeline.store.errors import StoreError

INDEX = "store_index"
VERSIONS = "_versions"
ONLY_SELECT = "only one SELECT is accepted"
# The table functions that run the SQL of a text as a statement of their own (DuckDB 2.0)
TEXT_SQL_FUNCTIONS = frozenset({"json_execute_serialized_sql", "query"})
TEXT_SQL_REFUSED = "query() and json_execute_serialized_sql() run SQL from text and are refused"
# No extension is installed, loaded by itself or taken from the community; the rest is set once
# connected, and then locked (see _settle)
CONFIG: dict[str, str | bool | int | float | list[str]] = {
    "autoinstall_known_extensions": False,
    "autoload_known_extensions": False,
    "allow_community_extensions": False,
}
SERIES_COLUMNS = ["key", "period", "date", "value", "projection"]
# The working columns of the views start with two underscores, as a source's columns never do
ID = "__id"
POSITION = "__position"
KNOWN = "__known_at"
CHOSEN = "__chosen"
BASE = "__rows_"


def quote(name: str) -> str:
    """A name as a DuckDB identifier, quoted whatever it holds."""
    return '"' + name.replace('"', '""') + '"'


def _text(value: str) -> str:
    """Text as a DuckDB literal."""
    return "'" + value.replace("'", "''") + "'"


def answer(storage: st.Storage, query: str, moment: pd.Timestamp | None) -> pd.DataFrame:
    """The rows of `query`, a single SELECT, over the views of `storage` (see the module),
    each source read as of `moment` when one is given. StoreError for anything but one SELECT,
    before it runs, for a source whose files DuckDB cannot read, and for a query DuckDB refuses,
    with its message and the names defined."""
    series = storage.series_sources()
    tables = storage.table_sources()
    names = sorted([*series, *tables, *(source + VERSIONS for source in [*series, *tables]), INDEX])
    builders = [(source, _series_views) for source in series] + [(source, _table_views) for source in tables]
    with (
        tempfile.TemporaryDirectory(prefix="data-pipeline-sql-") as spill,
        duckdb.connect(":memory:", config=CONFIG) as connection,
    ):
        try:
            statement = _check(connection, query, names)
            _settle(connection, pathlib.Path(spill))
            for source, build in builders:
                try:
                    build(connection, storage, source, moment)
                except duckdb.Error as exc:
                    msg = f"could not read source {source}: {exc}"
                    raise StoreError(msg) from exc
            connection.register(INDEX, storage.read_index())
            # Wrapped, DuckDB refuses what the parser calls a SELECT and is not one
            # No newline before the statement, so DuckDB reports the line of a one-line query as 1;
            # one before the closing parenthesis, so a `--` comment at the end cannot eat it
            return connection.execute(f"SELECT * FROM ({statement}\n)").df()  # noqa: S608 - the checked statement
        except duckdb.Error as exc:
            raise _failed(exc, names) from exc


def _settle(connection: duckdb.DuckDBPyConnection, spill: pathlib.Path) -> None:
    """Time in UTC, as the store writes it; anything spilled to disk in a folder of its own; no
    replacement scan of the caller's variables; and the configuration locked, last."""
    connection.execute("SET TimeZone = 'UTC'")
    connection.execute(f"SET temp_directory = {_text(spill.as_posix())}")
    with suppress(duckdb.Error):  # a DuckDB without the setting has no such scan to turn off
        connection.execute("SET python_enable_replacements = false")
    connection.execute("SET lock_configuration = true")


def _failed(error: duckdb.Error, names: Sequence[str]) -> StoreError:
    return StoreError(f"the query failed: {error}\nthe names a query can read: {', '.join(names)}")


def _check(connection: duckdb.DuckDBPyConnection, query: str, names: Sequence[str]) -> str:
    """Refuse, before anything runs, a query that is not exactly one SELECT. Returns that
    statement without the `;` and the comments that end it."""
    try:
        statements = connection.extract_statements(query)
    except duckdb.Error as exc:
        raise _failed(exc, names) from exc
    if not statements:
        msg = f"{ONLY_SELECT}, and the query is empty"
        raise StoreError(msg)
    if len(statements) > 1:
        msg = f"{ONLY_SELECT}, not {len(statements)} statements"
        raise StoreError(msg)
    kind = statements[0].type
    if kind == duckdb.StatementType.MULTI:
        msg = (
            f"{ONLY_SELECT}, not MULTI "
            "(a PIVOT without an IN list is several statements: list its values with IN (...))"
        )
        raise StoreError(msg)
    if kind != duckdb.StatementType.SELECT:
        msg = f"{ONLY_SELECT}, not {kind.name}"
        raise StoreError(msg)
    _refuse_text_sql(statements[0].query)
    return _without_tail(statements[0].query)


def _refuse_text_sql(statement: str) -> None:
    """StoreError for a call to a function that runs SQL from a text (TEXT_SQL_FUNCTIONS): a word
    of the statement that is one of those names, quoted or not, in any case, after any `schema.`,
    followed (past comments) by a parenthesis. The same word not followed by one, a column named
    `query` say, is not a call; a word inside a string is not a word."""
    tokens = duckdb.tokenize(statement)
    ends = [start for start, _ in tokens[1:]] + [len(statement)]
    words = [(kind, statement[start:end].strip()) for (start, kind), end in zip(tokens, ends, strict=True)]
    for index, (kind, word) in enumerate(words):
        if kind not in (duckdb.token_type.identifier, duckdb.token_type.keyword):
            continue
        name = word[1:-1].replace('""', '"') if len(word) > 1 and word[0] == word[-1] == '"' else word
        if name.lower() not in TEXT_SQL_FUNCTIONS:
            continue
        after = next((text for kind_after, text in words[index + 1 :] if kind_after != duckdb.token_type.comment), "")
        if after.startswith("("):
            raise StoreError(TEXT_SQL_REFUSED)


def _without_tail(statement: str) -> str:
    """The statement without the comments and the `;` after its last word."""
    end = len(statement)
    for start, kind in reversed(duckdb.tokenize(statement)):
        if kind != duckdb.token_type.comment and statement[start:end].strip() != ";":
            break
        end = start
    return statement[:end]


def _base(
    connection: duckdb.DuckDBPyConnection, source: str, paths: Sequence[pathlib.Path], expected: Sequence[str]
) -> str:
    """Create the hidden view of a source's rows, which both of its views read, and return its
    name. Every row of the Parquet files, read together by column name, with `__id`, the name of
    its file, and `__position`, where it sits in that file; a column of `expected` that no file
    has is missing in every row. The stamps are cast to TIMESTAMPTZ: DuckDB reads them as
    TIMESTAMPTZ_NS, which compares with no other time and reaches pandas without its zone (the
    store's stamps are whole microseconds, so nothing is lost).

    The view is made once for the files as they are (every file the store writes has the stamps),
    and made again, with the missing columns, only when it lacks one: reading the files' columns
    first would read every file's footer once more."""
    listed = ", ".join(_text(path.as_posix()) for path in paths)
    read = f"read_parquet([{listed}], union_by_name = true, filename = true, file_row_number = true)"
    casts = ", ".join(f"{quote(column)}::TIMESTAMPTZ AS {quote(column)}" for column in st.STAMP_COLUMNS)
    name = quote(BASE + source)

    def create(missing: str) -> None:
        connection.execute(
            f"CREATE OR REPLACE VIEW {name} AS SELECT parse_filename(filename, true) AS {quote(ID)}, "  # noqa: S608
            f"* EXCLUDE (filename, file_row_number) REPLACE ({casts}), "
            f"file_row_number AS {quote(POSITION)}{missing} FROM {read}"
        )

    create("")
    found = connection.execute("SELECT column_name FROM duckdb_columns() WHERE table_name = ?", [BASE + source])
    present = {row[0] for row in found.fetchall()}
    absent = [column for column in expected if column not in present]
    if absent:
        create("".join(f", NULL AS {quote(column)}" for column in absent))
    return name


def _chosen(
    base: str,
    columns: Sequence[str],
    key: Sequence[str],
    order: Sequence[str],
    moment: pd.Timestamp | None,
    *,
    by_publication: bool,
    keep: str = "true",
) -> str:
    """A SELECT of `columns` (SQL, as selected) from `base`: per `key` the row storage.latest
    chooses (or, given a moment, storage.as_of), among those that `keep`, sorted by `order` as
    those functions sort.

    `base` holds `__position`: of rows that tie on every stamp, pandas keeps the later one.
    """
    known = quote(KNOWN)
    position = quote(POSITION)
    if moment is None and not by_publication:
        # the row fetched last, then published last; a missing stamp is the earliest
        rank = "fetched_at DESC NULLS LAST, published_at DESC NULLS LAST"
        stamps = "fetched_at NULLS FIRST, published_at NULLS FIRST"
        visible = "true"
    else:
        # the row known last, then fetched last; a missing stamp is the latest
        rank = f"{known} DESC NULLS FIRST, fetched_at DESC NULLS FIRST"
        stamps = f"{known} NULLS LAST, fetched_at NULLS LAST"
        visible = "true" if moment is None else f"{known} <= TIMESTAMPTZ {_text(moment.floor('us').isoformat())}"
    partition = ", ".join(quote(column) for column in key)
    sort = ", ".join(f"{quote(column)} NULLS LAST" for column in order)
    return (
        f"SELECT {', '.join(columns)} FROM ("  # noqa: S608 - quoted names
        f"SELECT *, row_number() OVER (PARTITION BY {partition} ORDER BY {rank}, {position} DESC) AS {quote(CHOSEN)} "
        f"FROM (SELECT *, coalesce(published_at, fetched_at) AS {known} FROM {base}) WHERE {visible}"
        f") WHERE {quote(CHOSEN)} = 1 AND {keep} ORDER BY {sort}, {stamps}, {position}"
    )


def _series_views(
    connection: duckdb.DuckDBPyConnection, storage: st.Storage, source: str, moment: pd.Timestamp | None
) -> None:
    base = _base(connection, source, [storage.series_path(source)], SERIES_COLUMNS)
    every = ", ".join(quote(column) for column in [*SERIES_COLUMNS, *st.STAMP_COLUMNS])
    connection.execute(
        f"CREATE VIEW {quote(source + VERSIONS)} AS SELECT {every} FROM {base} "  # noqa: S608 - quoted names
        f'ORDER BY "key", "date", fetched_at NULLS FIRST, published_at NULLS FIRST, {quote(POSITION)}'
    )
    current = _chosen(
        base,
        [quote(column) for column in ["key", "date", "period", "value"]],
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
    paths = [storage.table_path(source, name) for name in storage.table_names(source)]
    base = _base(connection, source, paths, schema.columns)
    public = f"{quote(ID)} AS id"  # the id of the catalog entry whose table holds the row
    every = ", ".join([public, *(quote(column) for column in [*schema.columns, *st.STAMP_COLUMNS])])
    connection.execute(
        f"CREATE VIEW {quote(source + VERSIONS)} AS SELECT {every} "  # noqa: S608 - quoted names
        f"FROM {base} ORDER BY {quote(ID)}, {quote(POSITION)}"
    )
    current = _chosen(
        base,
        [public, *(quote(column) for column in schema.columns)],
        [ID, *schema.key_columns],
        [ID, *schema.row_order],
        moment,
        by_publication=schema.versioned,
    )
    connection.execute(f"CREATE VIEW {quote(source)} AS {current}")

