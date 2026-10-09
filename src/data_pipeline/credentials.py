"""The keys every part of the library uses, where they come from, and how they are saved.

A value passed in code wins over the process environment, which wins over the nearest `.env`:
the one in the current folder, else in its parent, and so on up to the root of the project (the
closest folder with `.git`, else the closest with `pyproject.toml`), never above it. Outside a
project only the current folder's `.env` counts, and the home folder's never does from below it:
a `.env` that belongs to another project, to the home folder or to a shared folder is neither
read nor written.
Credentials are personal: `repr` shows only which ones are present, never their values.

This module imports nothing from the store: the store and the command line both read it. Its
lock and its atomic write are the store's own, from `data_pipeline._files`.
"""

import contextlib
import dataclasses
import os
import pathlib
import re
import stat
import sys
import time
import unicodedata
from collections.abc import Iterator, Mapping

import dotenv

from data_pipeline._files import try_lock, unlock, write_atomic

ENV_FILE_NAME = ".env"
LOCK_SUFFIX = ".lock"  # saves to `.env` take turns on `.env.lock`
LOCK_TIMEOUT = 10.0  # seconds a save waits for another one to finish
LOCK_POLL = 0.05  # seconds between two tries of the lock
FILE_MODE = 0o600  # on POSIX, a new file: readable and writable by the owner only
# On POSIX, what a save keeps of an existing file's mode: the owner's and the group's bits, nothing
# for others, no setuid, setgid or sticky bit.
KEPT_MODE_BITS = stat.S_IRWXU | stat.S_IRWXG
ENV_FILE_ENCODING = "utf-8-sig"  # plain UTF-8, tolerating the BOM some editors add
# The root of a project: the closest folder with `.git`; with no `.git` above, the closest with
# `pyproject.toml`. Markers in order of precedence.
PROJECT_MARKERS = (".git", "pyproject.toml")
LINE_BREAK = re.compile(r"\r\n|\r|\n")  # the only line breaks python-dotenv knows
# Unicode categories a saved value may not hold: control characters (line feeds, tabs, NEL, VT,
# FS...) and the line and paragraph separators U+2028 and U+2029.
UNSAFE_CATEGORIES = frozenset({"Cc", "Zl", "Zp"})
ORIGIN_CODE = "code"
ORIGIN_ENVIRONMENT = "environment"
SETUP_COMMAND = "data-pipeline setup"

FRED = "FRED_API_KEY"
BLS = "BLS_API_KEY"
BANXICO = "BANXICO_TOKEN"
INEGI = "INEGI_TOKEN"
COMTRADE = "COMTRADE_API_KEY"
SEC_UA = "SEC_EDGAR_UA"


@dataclasses.dataclass(frozen=True)
class Key:
    name: str
    label: str  # the source, as people know it
    scope: str  # what it unlocks
    how: str  # how to get it, as the start of a sentence
    secret: bool = True


REGISTRY: tuple[Key, ...] = (
    Key(
        FRED,
        "FRED",
        "United States - macro",
        "Get one at https://fredaccount.stlouisfed.org/apikey",
    ),
    Key(
        BLS,
        "BLS",
        "United States - labour & prices",
        "Get one at https://data.bls.gov/registrationEngine/",
    ),
    Key(
        BANXICO,
        "Banxico SIE",
        "Mexico - rates & FX",
        "Get one at https://www.banxico.org.mx/SieAPIRest/service/v1/token",
    ),
    Key(
        INEGI,
        "INEGI",
        "Mexico - CPI & jobs",
        "Get one at https://www.inegi.org.mx/app/api/indicadores/interna_v1_1/tokenVerify.aspx",
    ),
    Key(
        COMTRADE,
        "UN Comtrade",
        "Goods trade",
        "Get one at https://comtradedeveloper.un.org/",
    ),
    Key(
        SEC_UA,
        "SEC EDGAR",
        "Filings & XBRL facts",
        "Set it to 'Your Name you@domain.com' (https://www.sec.gov/os/accessing-edgar-data)",
        secret=False,
    ),
)
NAMES: tuple[str, ...] = tuple(key.name for key in REGISTRY)
_BY_NAME: Mapping[str, Key] = {key.name: key for key in REGISTRY}


def key(name: str) -> Key:
    """The registry record of `name`. Raises KeyError for a name outside the registry."""
    return _BY_NAME[name]


def missing_message(name: str) -> str:
    """What to tell someone whose `name` is not set."""
    return f"{name} is missing. {key(name).how} and run: {SETUP_COMMAND}"


@dataclasses.dataclass(frozen=True, repr=False)
class Credentials:
    values: Mapping[str, str] = dataclasses.field(default_factory=dict)
    origins: Mapping[str, str] = dataclasses.field(default_factory=dict)

    def get(self, name: str) -> str | None:
        return self.values.get(name) or None

    def origin(self, name: str) -> str | None:
        """`code`, `environment`, or the path of the `.env` the value came from."""
        return self.origins.get(name) if self.get(name) else None

    def secrets(self) -> tuple[str, ...]:
        """The values to hide in messages and logs; the SEC User-Agent is sent in clear by design."""
        return tuple(
            value for name, value in self.values.items() if value and (name not in _BY_NAME or _BY_NAME[name].secret)
        )

    def __repr__(self) -> str:
        present = ", ".join(f"{name}={'yes' if self.get(name) else 'no'}" for name in NAMES)
        return f"Credentials({present})"


def _home() -> pathlib.Path | None:
    try:
        return pathlib.Path.home().resolve()
    except (RuntimeError, OSError):  # no home folder is known, as in some containers
        return None


def _climb(folder: pathlib.Path) -> list[pathlib.Path]:
    """`folder`, then each of its parents. The home folder ends the climb: it counts only when it is `folder`."""
    home = _home()
    walked: list[pathlib.Path] = []
    for candidate in (folder, *folder.parents):
        if walked and candidate == home:
            break
        walked.append(candidate)
    return walked


def project_root(start: pathlib.Path | None = None) -> pathlib.Path | None:
    """The root of the project `start` (default: the current folder) is in, or None outside a project.

    The root is the closest folder at or above `start` that holds `.git` (a folder, or the file a
    worktree or submodule has). With no `.git` above, it is the closest folder with
    `pyproject.toml`: a package with its own `pyproject.toml` inside a repository belongs to the
    repository. Neither marker is looked for above the home folder.
    """
    walked = _climb((start or pathlib.Path.cwd()).resolve())
    for marker in PROJECT_MARKERS:
        for candidate in walked:
            if (candidate / marker).exists():
                return candidate
    return None


def _search_path(folder: pathlib.Path) -> list[pathlib.Path]:
    """The folders whose `.env` counts from `folder`: itself, then each parent up to the project root.

    Outside a project only `folder` itself counts.
    """
    root = project_root(folder)
    if root is None:
        return [folder]
    walked = _climb(folder)
    return walked[: walked.index(root) + 1]


def find_env_file(start: pathlib.Path | None = None) -> pathlib.Path | None:
    """The `.env` of `start` (default: the current folder) or of its closest parent in the same project."""
    folder = (start or pathlib.Path.cwd()).resolve()
    for candidate in _search_path(folder):
        path = candidate / ENV_FILE_NAME
        if path.is_file():
            return path
    return None


def target_env_file(start: pathlib.Path | None = None) -> pathlib.Path:
    """Where to save: the nearest `.env`, else a new one at the root of the project.

    Outside a project the new `.env` goes in `start` (default: the current folder) itself.
    """
    folder = (start or pathlib.Path.cwd()).resolve()
    return find_env_file(folder) or (project_root(folder) or folder) / ENV_FILE_NAME


def resolve(
    explicit: Mapping[str, str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    env_file: pathlib.Path | None = None,
    start: pathlib.Path | None = None,
) -> Credentials:
    """Every known credential with its origin. `env_file` overrides the search from `start`."""
    explicit = explicit or {}
    unknown = sorted(set(explicit) - set(NAMES))
    if unknown:
        msg = f"Unknown credential: {', '.join(unknown)}. Known: {', '.join(NAMES)}"
        raise ValueError(msg)
    env = os.environ if environ is None else environ
    path = env_file if env_file is not None else find_env_file(start)
    path = path.resolve() if path is not None else None
    from_file = (
        dotenv.dotenv_values(path, interpolate=False, encoding=ENV_FILE_ENCODING)
        if path is not None and path.is_file()
        else {}
    )
    values: dict[str, str] = {}
    origins: dict[str, str] = {}
    for name in NAMES:
        for origin, layer in ((ORIGIN_CODE, explicit), (ORIGIN_ENVIRONMENT, env), (str(path), from_file)):
            value = (layer.get(name) or "").strip()
            if value:
                values[name] = value
                origins[name] = origin
                break
    return Credentials(values, origins)


def invalid_value(value: str) -> str | None:
    """Why `value` cannot be saved in a `.env`, or None when it can."""
    if any(unicodedata.category(char) in UNSAFE_CATEGORIES for char in value):
        return "it holds a line break, a tab or another control character"
    return None


def save(env_file: pathlib.Path, updates: Mapping[str, str]) -> None:
    """Write `updates` into `env_file`, replacing their lines and keeping every other line.

    Saves to the same file take turns (see `locked`), and each one writes a temporary file in the
    same folder that then replaces `env_file`: a reader sees the old file or the new one, never
    half of one, and a save that fails leaves the old file as it was (`write_atomic`; on
    Windows, a replacement refused for about five seconds because a reader has the file open
    raises `_files.ReplaceRefusedError`, a PermissionError). On POSIX a new file is readable and
    writable by its owner only (0600); an existing one keeps its mode and group, minus any access
    for others (see `_keep_access`).
    """
    for name, value in updates.items():
        if name not in _BY_NAME:
            msg = f"Unknown environment variable: {name}"
            raise ValueError(msg)
        reason = invalid_value(value)
        if reason is not None:
            msg = f"Invalid value for {name}: {reason}"
            raise ValueError(msg)
    path = env_file.resolve()  # through a symbolic link, the file it points at gets the new content
    with locked(path):
        lines = _lines(path.read_text(encoding=ENV_FILE_ENCODING)) if path.is_file() else []
        remaining = dict(updates)
        written: set[str] = set()
        kept: list[str] = []
        for line in lines:
            line_name = _line_name(line)
            if line_name is not None and line_name in updates:
                if line_name not in written:  # the first line of a name takes the new value, later ones are dropped
                    written.add(line_name)
                    kept.append(f"{line_name}={_encode(updates[line_name])}")
                    remaining.pop(line_name)
                continue
            kept.append(line)
        kept.extend(f"{name}={_encode(value)}" for name, value in remaining.items())
        text = "\n".join(kept) + "\n"
        write_atomic(path, text.encode("utf-8"), mode=FILE_MODE, prepare=_keep_access)


def lock_file(env_file: pathlib.Path) -> pathlib.Path:
    """The file saves to `env_file` take turns on: `.env.lock`, next to `.env`."""
    path = env_file.resolve()
    return path.with_name(path.name + LOCK_SUFFIX)


@contextlib.contextmanager
def locked(env_file: pathlib.Path) -> Iterator[None]:
    """Hold the lock of `env_file`, waiting up to LOCK_TIMEOUT seconds for whoever holds it now.

    The lock is the operating system's, taken on `lock_file(env_file)` the way the store takes its
    own (`data_pipeline._files`): it ends with the process that holds it, so a save that crashes
    leaves no stale lock (the empty file stays and is reused). Raises TimeoutError when the wait
    runs out.
    """
    descriptor = os.open(lock_file(env_file), os.O_RDWR | os.O_CREAT, FILE_MODE)
    try:
        deadline = time.monotonic() + LOCK_TIMEOUT
        while not try_lock(descriptor):
            if time.monotonic() >= deadline:
                msg = f"{env_file} is being saved by another program; try again"
                raise TimeoutError(msg)
            time.sleep(LOCK_POLL)
        try:
            yield
        finally:
            unlock(descriptor)
    finally:
        os.close(descriptor)


def _keep_access(descriptor: int, path: pathlib.Path) -> None:
    """Give the open new copy of `path` the access the old one had, minus any for others (POSIX).

    A new file stays 0600, as it was created. An existing one keeps its mode (0640 so that a
    service's group reads it, say) without the bits for others, and keeps its group. When the
    group cannot be kept (its owner is not in that group), the group bits go too: the access
    never passes to another group. On Windows the new file takes its access from its folder, as
    the old one did, so there is nothing to do.
    """
    if sys.platform == "win32":
        return
    try:
        old = path.stat()
    except FileNotFoundError:
        return
    mode = stat.S_IMODE(old.st_mode) & KEPT_MODE_BITS
    if mode & stat.S_IRWXG and os.fstat(descriptor).st_gid != old.st_gid:
        try:
            os.fchown(descriptor, -1, old.st_gid)
        except PermissionError:
            mode &= ~stat.S_IRWXG
    os.fchmod(descriptor, mode)


def _lines(text: str) -> list[str]:
    """The lines of a `.env` as python-dotenv sees them: split on CR LF, CR or LF only.

    `str.splitlines` also splits on U+2028, U+0085, VT, FS and others, which would turn part of a
    value into a line of its own on the next save.
    """
    lines = LINE_BREAK.split(text)
    return lines[:-1] if lines[-1] == "" else lines


def _line_name(line: str) -> str | None:
    """The variable a `.env` line sets, or None for blanks, comments and anything else."""
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in stripped:
        return None
    name = stripped.partition("=")[0].strip()
    return name.removeprefix("export ").lstrip()


def _encode(value: str) -> str:
    """`value` as a `.env` right-hand side that reads back unchanged: quoted only when it must be."""
    if value != value.strip() or any(char in value for char in "#\"'\\"):
        return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return value
