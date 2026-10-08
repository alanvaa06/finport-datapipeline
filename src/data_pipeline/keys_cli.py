"""Console commands for the keys: setup fills the nearest .env, keys reports what is set.
Output is ASCII only."""

import pathlib

import click

from data_pipeline import credentials, keys_check
from data_pipeline.credentials import Key


def echo(line: str) -> None:
    """Print one line, replacing anything outside ASCII (Windows consoles default to cp1252)."""
    click.echo(line.encode("ascii", "replace").decode("ascii"))


@click.group()
def cli() -> None:
    """Keys of the data sources."""


@cli.command("keys")
def keys_command() -> None:
    """Which keys are set, and where each one comes from. Never shows a value."""
    env_file = credentials.find_env_file()
    resolved = credentials.resolve()
    echo(f".env: {env_file or 'none found here or in a parent folder of this project (run: data-pipeline setup)'}")
    for key in credentials.REGISTRY:
        origin = resolved.origin(key.name)
        mark = "[ok]" if origin else "[ ] "
        echo(f"{mark}  {key.name:<17} {key.label:<24} {origin or 'missing'}")


def _choose(raw: str, resolved: credentials.Credentials) -> list[Key]:
    if not raw.strip():
        return [key for key in credentials.REGISTRY if resolved.get(key.name) is None]
    chosen = []
    for part in raw.replace(" ", "").split(","):
        if not part.isdigit() or not 1 <= int(part) <= len(credentials.REGISTRY):
            msg = f"choose numbers from 1 to {len(credentials.REGISTRY)}, separated by commas"
            raise click.UsageError(msg)
        chosen.append(credentials.REGISTRY[int(part) - 1])
    return chosen


def _prompt_value(key: Key) -> str | None:
    """What the person typed for `key`, or None when they pressed Enter to skip it."""
    if key.name == credentials.SEC_UA:
        name = click.prompt(
            "Your name (the SEC asks who is downloading) (Enter to skip)", default="", show_default=False
        ).strip()
        if not name:
            return None
        email = click.prompt("Your e-mail", default="", show_default=False).strip()
        return " ".join(part for part in (name, email) if part)
    answer = click.prompt(f"{key.name} ({key.how}) (Enter to skip)", default="", show_default=False, hide_input=True)
    return str(answer).strip() or None


def _ask(key: Key, *, check: bool) -> str | None:
    """The value to save for `key`, or None when the person skips it or gives up on a rejected one."""
    while True:
        value = _prompt_value(key)
        if value is None:
            echo(f"skipped {key.name}")
            return None
        # The SEC needs a contact e-mail: that rule is local, so it applies even without a check.
        needs_email = key.name == credentials.SEC_UA and "@" not in value
        if not check and not needs_email:
            return value
        result = keys_check.check(key.name, value)
        echo(result.line())
        if result.verdict is not keys_check.Verdict.REJECTED:
            return value
        if not click.confirm("Type it again?", default=True):
            return None


@cli.command("setup")
@click.option("--no-check", is_flag=True, help="Save without asking each source whether the key works.")
def setup_command(*, no_check: bool) -> None:
    """Fill the nearest .env of this project with the keys you choose, checking each one."""
    env_file: pathlib.Path = credentials.target_env_file()
    echo(f"Keys are saved in {env_file}")
    # The search never leaves the project, but a parent folder's file is still not the one in
    # front of the person: they confirm it before any key goes there.
    if env_file.parent != pathlib.Path.cwd().resolve() and not click.confirm(
        "That .env is in a parent folder. Save the keys there?", default=True
    ):
        echo("Nothing saved. To keep the keys in this folder, create an empty .env here and run setup again.")
        return
    resolved = credentials.resolve(env_file=env_file)
    echo("")
    for number, key in enumerate(credentials.REGISTRY, start=1):
        state = "set" if resolved.get(key.name) else "missing"
        echo(f"  {number}  {key.name:<17} {key.label:<24} {key.scope:<32} {state}")
    raw = click.prompt(
        "Which ones? (numbers separated by commas, Enter for all missing)",
        default="",
        show_default=False,
    )
    chosen = _choose(raw, resolved)
    if not chosen:
        echo("Every key is set. Type its number to replace one.")
        return
    saved = False
    for key in chosen:
        origin = resolved.origin(key.name)
        if origin == credentials.ORIGIN_ENVIRONMENT:
            question = (
                f"{key.name} is set in the environment, which wins over {env_file.name}. Save one in .env anyway?"
            )
        elif origin:
            question = f"{key.name} is already set. Replace it?"
        else:
            question = ""
        if question and not click.confirm(question, default=False):
            continue
        value = _ask(key, check=not no_check)
        if value:
            credentials.save(env_file, {key.name: value})  # now, so an interruption keeps what was accepted
            saved = True
    echo(f"Saved in {env_file}." if saved else "Nothing saved.")
