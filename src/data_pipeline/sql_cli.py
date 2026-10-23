"""The `sql` command: one SELECT over the store, answered by DuckDB. Output is ASCII only.

Exit codes: 0 done, 1 the rows could not be written to --out, 2 a query refused or failed, or a
bad option.
"""

import pathlib

import click
import pandas as pd

from data_pipeline.fetching import WRITE_ERRORS, destination
from data_pipeline.store.api import Store
from data_pipeline.store.cli import PATH, SHOWN_ROWS, echo, fail, root_option
from data_pipeline.store.errors import StoreError
from data_pipeline.store.sync import EXIT_FAILURES, EXIT_OK

WHERE = "sql"
RESULT = "result"  # the table of a .duckdb file the rows go to


def cell(value: object) -> str:
    """A value as the console shows it: a day without its midnight, a moment in ISO form."""
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None and value == value.normalize():
            return value.date().isoformat()
        return value.isoformat()
    return str(value)


@click.group()
def cli() -> None:
    """SQL over the store."""


@cli.command("sql")
@root_option
@click.argument("query")
@click.option("--as-of", "as_of", default=None, help="Read each source as it was known on this date (YYYY-MM-DD).")
@click.option(
    "--out",
    type=PATH,
    default=None,
    help="Write every row to this .parquet or .csv file, replacing it, or to the table `result` of a .duckdb file.",
)
def sql_command(*, root: pathlib.Path, query: str, as_of: str | None, out: pathlib.Path | None) -> None:
    """Answer QUERY, one SELECT over the store: the first rows, or every row to --out.

    Each source with data is two names: SOURCE, its latest rows (as of --as-of when given), and
    SOURCE_versions, every row stored; store_index is the index. Nothing in the store changes.
    """
    try:
        target = destination(out, where=WHERE)
        data = Store(root).sql(query, as_of=as_of)
    except StoreError as exc:
        raise fail(exc) from exc
    echo(f"{len(data)} rows, {len(data.columns)} columns")
    if target is None:
        echo("  ".join(str(column) for column in data.columns))
        for values in data.head(SHOWN_ROWS).itertuples(index=False):
            echo("  ".join(cell(value) for value in values))
        if len(data) > SHOWN_ROWS:
            echo(f"[i]   the first {SHOWN_ROWS} of {len(data)} rows; use --out FILE for all of them")
        raise SystemExit(EXIT_OK)
    try:
        target.write(data, table=RESULT, by_id=False)
    except WRITE_ERRORS as exc:
        echo(f"[x]   could not write {out}: {type(exc).__name__}: {' '.join(str(exc).split())}")
        raise SystemExit(EXIT_FAILURES) from exc
    echo(f"[ok]  written to {out}")
    raise SystemExit(EXIT_OK)
