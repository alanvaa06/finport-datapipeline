"""Console commands for the keys: setup fills the nearest .env, keys reports what is set.
Output is ASCII only."""

import pathlib
import shutil
import subprocess

import click

from data_pipeline import credentials, keys_check
from data_pipeline.credentials import Key

GITIGNORE_FILE = ".gitignore"
GIT = "git"  # the git command, looked up on PATH; without it the checks that need git are skipped


def ascii_only(text: str) -> str:
    """`text` with anything outside ASCII replaced (Windows consoles default to cp1252)."""
    return text.encode("ascii", "replace").decode("ascii")


def echo(line: str) -> None:
    """Print one line, ASCII only."""
    click.echo(ascii_only(line))


def _git(folder: pathlib.Path, *args: str) -> subprocess.CompletedProcess[str] | None:
    """Run git with `args` in `folder`; None when git is not installed.

    `core.fsmonitor` is turned off so that no hook program configured in the repository runs.
    """
    executable = shutil.which(GIT)
    if executable is None:
        return None
    return subprocess.run(  # noqa: S603  # a fixed command, no shell
        [executable, "-c", "core.fsmonitor=false", *args],
        cwd=folder,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )


def _refuse_a_tracked_env_file(env_file: pathlib.Path) -> None:
    """Stop when git tracks `env_file`: no ignore rule applies to a tracked file, so the keys would be committed.

    Without git, or outside a repository, there is nothing to check.
    """
    result = _git(env_file.parent, "ls-files", "--error-unmatch", "--", env_file.name)
    if result is None or result.returncode != 0:
        return
    msg = (
        f"{env_file} is tracked by git, and no .gitignore rule applies to a tracked file: the keys would go"
        " into the next commit. Nothing was saved. To stop tracking it (the file stays on disk), run in"
        f" {env_file.parent}:\n\n    git rm --cached {env_file.name}\n\nthen run setup again. If real keys"
        " were ever committed, they are in the history: replace them at their source."
    )
    raise click.ClickException(ascii_only(msg))


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


def _keep_out_of_git(env_file: pathlib.Path) -> None:
    """List `env_file` and its lock file in the `.gitignore` of its folder, unless they are already.

    The `.gitignore` is only appended to, byte for byte: whatever its encoding or line endings,
    the existing content is left as it is.
    """
    gitignore = env_file.parent / GITIGNORE_FILE
    data = gitignore.read_bytes() if gitignore.is_file() else b""
    listed = {line.strip().removeprefix("/") for line in data.decode("utf-8", "replace").lstrip("\ufeff").splitlines()}
    missing = [name for name in (env_file.name, credentials.lock_file(env_file).name) if name not in listed]
    if not missing:
        return
    newline = b"\r\n" if b"\r\n" in data else b"\n"
    separator = newline if data and not data.endswith(b"\n") else b""
    with gitignore.open("ab") as handle:
        handle.write(separator + b"".join(name.encode("ascii") + newline for name in missing))
    echo(f"Added {', '.join(missing)} to {gitignore}")


@cli.command("setup")
@click.option("--no-check", is_flag=True, help="Save without asking each source whether the key works.")
def setup_command(*, no_check: bool) -> None:
    """Fill the .env of this project (the nearest one, else a new one at its root) with the keys you choose."""
    env_file: pathlib.Path = credentials.target_env_file()
    echo(f"Keys are saved in {env_file}")
    _refuse_a_tracked_env_file(env_file)
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
            if not saved:
                _keep_out_of_git(env_file)  # before the first key reaches the file
            credentials.save(env_file, {key.name: value})  # now, so an interruption keeps what was accepted
            saved = True
    echo(f"Saved in {env_file}." if saved else "Nothing saved.")
