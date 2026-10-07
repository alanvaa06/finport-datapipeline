"""The `data-pipeline` console script: the store's commands and the key commands under one name."""

import click

from data_pipeline import __version__, keys_cli
from data_pipeline.store.cli import cli as store_cli

cli = click.version_option(__version__, prog_name="data-pipeline")(
    click.CommandCollection(
        sources=[store_cli, keys_cli.cli],
        help="Public economic and company data, kept locally with every revision.",
    )
)
