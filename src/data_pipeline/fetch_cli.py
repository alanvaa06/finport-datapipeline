"""The `fetch` command: download from a source without a store. Output is ASCII only.

Exit codes, those of sync: 0 every id arrived, 1 some id failed, 2 configuration error, 3 the
source's quota ran out (and nothing else failed).
"""

import pathlib
from typing import Any

import click
import pandas as pd
import yaml

from data_pipeline.fetching import FetchError, fetch
from data_pipeline.store.cli import PATH, SHOWN_ROWS, echo, fail
from data_pipeline.store.errors import StoreError
from data_pipeline.store.model import Outcome
from data_pipeline.store.sync import EXIT_FAILURES, EXIT_OK, EXIT_QUOTA

# fetch's own arguments, which a --param cannot name: those the command has an option for, and the rest
OWN_OPTIONS = {"to": "--out", "start": "--start", "frequency": "--frequency", "env_file": "--env-file"}
RESERVED = frozenset({*OWN_OPTIONS, "credentials", "transport", "sleep"})


def read_params(pairs: tuple[str, ...]) -> dict[str, Any]:
    """NAME=VALUE pairs as the source's fields, each value read as YAML, as in a catalog."""
    params: dict[str, Any] = {}
    for pair in pairs:
        name, equals, value = pair.partition("=")
        name = name.strip()
        if not equals or not name:
            msg = f"{pair!r} is not NAME=VALUE"
            raise click.BadParameter(msg, param_hint="--param")
        if name in RESERVED:
            own = OWN_OPTIONS.get(name)
            msg = f"use {own}, not --param {name}=" if own else f"{name!r} cannot be given as --param"
            raise click.BadParameter(msg, param_hint="--param")
        try:
            params[name] = yaml.safe_load(value)
        except yaml.YAMLError as exc:
            msg = f"the value of {name!r} is not valid YAML: {value!r}"
            raise click.BadParameter(msg, param_hint="--param") from exc
    return params


def _cell(value: object) -> str:
    return value.date().isoformat() if isinstance(value, pd.Timestamp) else str(value)


def show(source: str, data: pd.DataFrame, out: pathlib.Path | None) -> None:
    """The summary line, then the first rows when there is no file to write them to."""
    ids = data["id"].nunique() if "id" in data else 0
    echo(f"{source}: {ids} ids, {len(data)} rows")
    if out is not None or data.empty:
        return
    echo("  ".join(data.columns))
    for values in data.head(SHOWN_ROWS).itertuples(index=False):
        echo("  ".join(_cell(value) for value in values))
    if len(data) > SHOWN_ROWS:
        echo(f"[i]   the first {SHOWN_ROWS} of {len(data)} rows; use --out FILE for all of them")


@click.group()
def cli() -> None:
    """Downloading without a store."""


@cli.command("fetch")
@click.argument("source")
@click.argument("ids", nargs=-1, required=True)
@click.option(
    "--out",
    type=PATH,
    default=None,
    help="Write every row to this .parquet or .csv file, replacing it, or to the table SOURCE of a .duckdb file.",
)
@click.option("--start", default=None, help="Ask from this date on (YYYY-MM-DD), where the source takes one.")
@click.option("--frequency", default=None, help="D, W, M, Q or A, for a source that does not report one.")
@click.option("--param", "params", multiple=True, help="A field of the source, NAME=VALUE, read as YAML. Repeatable.")
@click.option(
    "--env-file",
    type=PATH,
    default=None,
    help="File with the credentials. Defaults to the nearest .env (this folder or a parent).",
)
def fetch_command(
    *,
    source: str,
    ids: tuple[str, ...],
    out: pathlib.Path | None,
    start: str | None,
    frequency: str | None,
    params: tuple[str, ...],
    env_file: pathlib.Path | None,
) -> None:
    """Download IDS of SOURCE without a store: the first rows, or every row to --out.

    A series comes as id, date, period, value; a table as id and the columns of its source.
    Nothing is stored and there are no versions: every run downloads again and spends the
    source's quota again. For data read often, keep a store with sync.
    """
    fields = read_params(params)
    try:
        data = fetch(source, list(ids), to=out, start=start, frequency=frequency, env_file=env_file, **fields)
    except FetchError as exc:
        show(source, exc.data, out)
        if out is not None and exc.written is not None:
            echo(f"[i]   what arrived is written to {out}")
        elif out is not None and not exc.write_error:
            echo(f"[i]   nothing was written to {out}")  # nothing arrived: the file is as it was
        for failure in exc.failures:
            echo(f"[x]   {failure.entry.key}  {failure.outcome.value}: {failure.reason}")
        if exc.write_error:
            echo(f"[x]   could not write {out}: {exc.write_error}")
        only_quota = all(failure.outcome is Outcome.QUOTA_EXHAUSTED for failure in exc.failures)
        raise SystemExit(EXIT_QUOTA if exc.quota_exhausted and only_quota else EXIT_FAILURES) from exc
    except StoreError as exc:
        raise fail(exc) from exc
    show(source, data, out)
    if out is not None:
        echo(f"[ok]  written to {out}")
    raise SystemExit(EXIT_OK)
