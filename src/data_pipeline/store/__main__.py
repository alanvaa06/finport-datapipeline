"""Run the store commands without the console script:  python -m data_pipeline.store sync ..."""

from data_pipeline.store.cli import cli

cli()
