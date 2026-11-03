"""Console commands for the keys: setup fills the nearest .env, keys reports what is set.
Output is ASCII only."""

import dataclasses
import pathlib
import shutil
import subprocess

import click

from data_pipeline import credentials, keys_check
from data_pipeline.credentials import Key

GITIGNORE_FILE = ".gitignore"
GIT = "git"  # the git command, looked up on PATH
NUL = chr(0)  # separates the paths given to, and the fields printed by, `git check-ignore -z`
BOM = chr(0xFEFF)  # the byte order mark some editors put at the start of a .gitignore


def ascii_only(text: str) -> str:
    """`text` with anything outside ASCII replaced (Windows consoles default to cp1252)."""
    return text.encode("ascii", "replace").decode("ascii")


def echo(line: str) -> None:
    """Print one line, ASCII only."""
    click.echo(ascii_only(line))


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


def _local_check(key: Key, value: str) -> keys_check.Check | None:
    """The rejection of `value` that needs no request to its source, or None.

    These rules apply even without a check: a value `credentials.save` refuses (it is never sent
    to its source either), and an SEC contact with no e-mail.
    """
    reason = credentials.invalid_value(value)
    if reason is not None:
        return keys_check.Check(key.name, keys_check.Verdict.REJECTED, reason)
    if key.name == credentials.SEC_UA and "@" not in value:
        return keys_check.check(key.name, value)  # rejected locally, with the SEC's wording
    return None


def _ask(key: Key, *, check: bool) -> str | None:
    """The value to save for `key`, or None when the person skips it or gives up on a rejected one.

    Whitespace around what was typed or pasted is dropped (`_prompt_value`).
    """
    while True:
        value = _prompt_value(key)
        if value is None:
            echo(f"skipped {key.name}")
            return None
        result = _local_check(key, value)
        if result is None and check:
            result = keys_check.check(key.name, value)
        if result is None:
            return value
        echo(result.line())
        if result.verdict is not keys_check.Verdict.REJECTED:
            return value
        if not click.confirm("Type it again?", default=True):
            return None


def _save(env_file: pathlib.Path, key: Key, value: str) -> bool:
    """Save `value` now; when the file cannot be written, say why and offer to try again.

    False when the person gives up on that key. The usual reason is another program saving the
    same file (`credentials.save` raises TimeoutError after waiting for it).
    """
    while True:
        try:
            credentials.save(env_file, {key.name: value})
        except OSError as exc:  # TimeoutError included
            echo(f"[x]   {key.name}  not saved: {exc}")
            if not click.confirm("Try again?", default=True):
                echo(f"skipped {key.name}")
                return False
        else:
            return True


def _git(folder: pathlib.Path, *args: str, stdin: str | None = None) -> subprocess.CompletedProcess[str] | None:
    """Run git with `args` in `folder`, feeding it `stdin`; None when git is not installed.

    `core.fsmonitor` is turned off so that no hook program configured in the repository runs.
    """
    executable = shutil.which(GIT)
    if executable is None:
        return None
    return subprocess.run(  # noqa: S603  # a fixed command, no shell
        [executable, "-c", "core.fsmonitor=false", *args],
        cwd=folder,
        input=stdin,
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


@dataclasses.dataclass(frozen=True)
class _Rule:
    """The last ignore rule that matches a path, as `git check-ignore -v` reports it; all blank when none does."""

    source: str
    line: str
    pattern: str

    def describe(self) -> str:
        return f"The rule {self.pattern} in {self.source} (line {self.line})"


def _git_rules(folder: pathlib.Path, names: tuple[str, ...]) -> dict[str, _Rule] | None:
    """The rule git applies to each of `names` in `folder`, or None when git cannot tell.

    git cannot tell when it is not installed or `folder` is in no repository. Every ignore rule
    it knows counts: any `.gitignore` up the tree, `.git/info/exclude`, the user's global one.
    """
    result = _git(folder, "check-ignore", "--stdin", "-z", "-v", "-n", stdin="".join(name + NUL for name in names))
    if result is None or result.returncode not in (0, 1):
        return None
    fields = result.stdout.split(NUL)  # source, line, pattern, path for each path; a final empty field
    return {fields[i + 3]: _Rule(*fields[i : i + 3]) for i in range(0, len(fields) - 3, 4)}


def _refuse_an_un_ignored_env_file(env_file: pathlib.Path, rule: str) -> None:
    msg = (
        f"{rule} un-ignores {env_file.name}, so git would commit the keys saved in {env_file}. Nothing was"
        " saved: setup does not override that rule. Remove it and run setup again, or set the keys as"
        " environment variables instead."
    )
    raise click.ClickException(ascii_only(msg))


def _gitignore_lines(gitignore: pathlib.Path) -> set[str]:
    data = gitignore.read_bytes() if gitignore.is_file() else b""
    return {line.strip() for line in data.decode("utf-8", "replace").lstrip(BOM).splitlines()}


def _gitignore_additions(env_file: pathlib.Path) -> list[str]:
    """The names the `.gitignore` next to `env_file` needs so that git ignores it and its lock file.

    git decides; outside a repository nothing is needed. Without git, only inside a project whose
    root holds `.git`, the names not listed yet as lines of that `.gitignore`. Raises
    ClickException when a rule un-ignores the `.env`: setup never overrides someone's `!.env`.
    """
    names = (env_file.name, env_file.name + credentials.LOCK_SUFFIX)
    rules = _git_rules(env_file.parent, names)
    if rules is not None:
        rule = rules.get(env_file.name)
        if rule is not None and rule.pattern.startswith("!"):
            _refuse_an_un_ignored_env_file(env_file, rule.describe())
        # No rule at all: add one. A rule that un-ignores the lock file is someone's choice: keep it.
        return [name for name in names if name not in rules or not rules[name].pattern]
    root = credentials.project_root(env_file.parent)
    if root is None or not (root / ".git").exists():
        return []
    gitignore = env_file.parent / GITIGNORE_FILE
    lines = _gitignore_lines(gitignore)
    if {f"!{env_file.name}", f"!/{env_file.name}"} & lines:
        _refuse_an_un_ignored_env_file(env_file, f"A !{env_file.name} line in {gitignore}")
    listed = {line.removeprefix("/") for line in lines}
    return [name for name in names if name not in listed]


def _add_to_gitignore(folder: pathlib.Path, names: list[str]) -> None:
    """Append `names` to the `.gitignore` of `folder`, creating it if needed.

    The `.gitignore` is only appended to, byte for byte: whatever its encoding or line endings,
    the existing content is left as it is.
    """
    if not names:
        return
    gitignore = folder / GITIGNORE_FILE
    data = gitignore.read_bytes() if gitignore.is_file() else b""
    newline = b"\r\n" if b"\r\n" in data else b"\n"
    separator = newline if data and not data.endswith(b"\n") else b""
    with gitignore.open("ab") as handle:
        handle.write(separator + b"".join(name.encode("ascii") + newline for name in names))
    echo(f"Added {', '.join(names)} to {gitignore}")


@cli.command("setup")
@click.option("--no-check", is_flag=True, help="Save without asking each source whether the key works.")
def setup_command(*, no_check: bool) -> None:
    """Fill the .env of this project (the nearest one, else a new one at its root) with the keys you choose."""
    env_file: pathlib.Path = credentials.target_env_file()
    echo(f"Keys are saved in {env_file}")
    _refuse_a_tracked_env_file(env_file)
    ignore = _gitignore_additions(env_file)
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
            _add_to_gitignore(env_file.parent, ignore)  # before the first key reaches the file
            ignore = []
            if _save(env_file, key, value):  # now, so an interruption keeps what was accepted
                saved = True
    echo(f"Saved in {env_file}." if saved else "Nothing saved.")
