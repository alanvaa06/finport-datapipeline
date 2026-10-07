"""Console commands of the store: sync, status, show. Output is ASCII only.

Exit codes: 0 everything is up to date, 1 there were failures, 2 configuration error,
3 incomplete because of a quota (run again tomorrow).
"""

import pathlib

import click

from data_pipeline.store.api import STATE_OK, Store
from data_pipeline.store.errors import StoreError
from data_pipeline.store.storage import KIND_DOCUMENT, KIND_TABLE
from data_pipeline.store.sync import EXIT_CONFIGURATION, EXIT_FAILURES, EXIT_OK

ROOT_VARIABLE = "DATA_PIPELINE_ROOT"
SHOWN_ROWS = 10
PATH = click.Path(path_type=pathlib.Path)

root_option = click.option(
    "--root",
    envvar=ROOT_VARIABLE,
    required=True,
    type=PATH,
    help=f"Folder of the store. Defaults to the {ROOT_VARIABLE} environment variable.",
)


def echo(line: str) -> None:
    """Print one line, replacing anything outside ASCII (Windows consoles default to cp1252)."""
    click.echo(line.encode("ascii", "replace").decode("ascii"))


def open_store(root: pathlib.Path, catalog: pathlib.Path | None, env_file: pathlib.Path | None) -> Store:
    return Store(root, catalog, env_file=env_file)


def fail(error: Exception) -> SystemExit:
    echo(f"[x]   {error}")
    return SystemExit(EXIT_CONFIGURATION)


@click.group()
def cli() -> None:
    """finport-datapipeline: local store of public economic data."""


@cli.command("sync")
@root_option
@click.option("--catalog", type=PATH, default=pathlib.Path("catalog.yaml"), show_default=True, help="YAML catalog.")
@click.option("--source", "sources", multiple=True, help="Sync only this source. Repeatable.")
@click.option("--full", is_flag=True, help="Ask for every series' whole history again; store only what changed.")
@click.option(
    "--env-file",
    type=PATH,
    default=None,
    help="File with the credentials. Defaults to the nearest .env (this folder or a parent).",
)
def sync_command(
    *,
    root: pathlib.Path,
    catalog: pathlib.Path,
    sources: tuple[str, ...],
    full: bool,
    env_file: pathlib.Path | None,
) -> None:
    """Download what the catalog declares and store what changed."""
    try:
        report = open_store(root, catalog, env_file).sync(sources=sources or None, full=full)
    except StoreError as exc:
        raise fail(exc) from exc
    for line in report.lines():
        echo(line)
    raise SystemExit(report.exit_code)


@cli.command("status")
@root_option
@click.option("--catalog", type=PATH, default=None, help="YAML catalog; needed to report 'missing' series.")
def status_command(*, root: pathlib.Path, catalog: pathlib.Path | None) -> None:
    """Freshness of every stored series."""
    try:
        table = open_store(root, catalog, None).status()
    except StoreError as exc:
        raise fail(exc) from exc
    for row in table.to_dict(orient="records"):
        if row["state"] == STATE_OK:
            fetched = row["last_fetched_at"].date().isoformat()
            echo(f"[ok]  {row['key']}  {row['last_period']}  fetched {fetched}")
        else:
            echo(f"[x]   {row['key']}  {row['state']}  {row['reason']}")
    all_ok = bool((table["state"] == STATE_OK).all())
    raise SystemExit(EXIT_OK if all_ok else EXIT_FAILURES)


@cli.command("show")
@root_option
@click.argument("key")
@click.option("--as-of", "as_of", default=None, help="Show the data as it was known on this date (YYYY-MM-DD).")
def show_command(*, root: pathlib.Path, key: str, as_of: str | None) -> None:
    """Print the citation of a series, a table or a set of documents and its newest rows."""
    try:
        store = open_store(root, None, None)
        info = store.info(key)
        if info.kind == KIND_TABLE:
            table = store.table(info.source, info.source_id, as_of=as_of)
        elif info.kind == KIND_DOCUMENT:
            files = store.documents(info.source, info.source_id, as_of=as_of)
        else:
            rows = store.series(key, as_of=as_of)
    except StoreError as exc:
        raise fail(exc) from exc
    echo(info.label)
    if info.kind == KIND_DOCUMENT:
        echo(f"{info.name} | {len(files)} files")
        shown = files.drop(columns=["id", "url", "size", "sha256", "fetched_at", "path"]).tail(SHOWN_ROWS)
        shown = shown.assign(date=shown["date"].dt.date)
        echo("  ".join(shown.columns))
        for values in shown.itertuples(index=False):
            echo("  ".join(str(value) for value in values))
        return
    if info.kind == KIND_TABLE:
        echo(f"{info.name} | {len(table)} rows")
        newest = table.sort_values("date", kind="stable").tail(SHOWN_ROWS).drop(columns="date")
        echo("  ".join(newest.columns))
        for values in newest.itertuples(index=False):
            echo("  ".join(str(value) for value in values))
        return
    echo(f"{info.name} | {info.units} | {info.frequency} | {info.seasonal_adjustment}")
    for row in rows.tail(SHOWN_ROWS).to_dict(orient="records"):
        echo(f"{row['period']}  {row['value']}")
