"""The keys every part of the library uses, where they come from, and how they are saved.

A value passed in code wins over the process environment, which wins over the nearest `.env`:
the one in the current folder, else in its parent, and so on up to the root of the project (the
folder with `.git` or `pyproject.toml`), never above it. Outside a project only the current
folder's `.env` counts, and the home folder's never does from below it: a `.env` that belongs to
another project, to the home folder or to a shared folder is neither read nor written.
Credentials are personal: `repr` shows only which ones are present, never their values.

This module imports nothing from the store: the store and the command line both read it.
"""

import contextlib
import dataclasses
import errno
import os
import pathlib
import re
import sys
import tempfile
import time
import unicodedata
from collections.abc import Iterator, Mapping

import dotenv

if sys.platform == "win32":
    import msvcrt
else:
    import fcntl

ENV_FILE_NAME = ".env"
LOCK_SUFFIX = ".lock"  # saves to `.env` take turns on `.env.lock`
LOCK_TIMEOUT = 10.0  # seconds a save waits for another one to finish
LOCK_POLL = 0.05  # seconds between two tries of the lock
REPLACE_ATTEMPTS = 10
REPLACE_WAIT = 0.5  # seconds between attempts: about five seconds in all
FILE_MODE = 0o600  # on POSIX: readable and writable by the owner only
ENV_FILE_ENCODING = "utf-8-sig"  # plain UTF-8, tolerating the BOM some editors add
PROJECT_MARKERS = (".git", "pyproject.toml")  # a folder holding either is the root of a project
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


def _search_path(folder: pathlib.Path) -> list[pathlib.Path]:
    """The folders whose `.env` counts from `folder`: itself, then each parent up to the project root.

    Outside a project (no `.git` or `pyproject.toml` at `folder` or above it, below the home
    folder) only `folder` itself counts. The home folder ends the climb: it counts only when it
    is `folder`.
    """
    home = _home()
    walked: list[pathlib.Path] = []
    for candidate in (folder, *folder.parents):
        if walked and candidate == home:
            break
        walked.append(candidate)
        if any((candidate / marker).exists() for marker in PROJECT_MARKERS):
            return walked
    return walked[:1]


def find_env_file(start: pathlib.Path | None = None) -> pathlib.Path | None:
    """The `.env` of `start` (default: the current folder) or of its closest parent in the same project."""
    folder = (start or pathlib.Path.cwd()).resolve()
    for candidate in _search_path(folder):
        path = candidate / ENV_FILE_NAME
        if path.is_file():
            return path
    return None


def target_env_file(start: pathlib.Path | None = None) -> pathlib.Path:
    """Where to save: the nearest `.env`, or a new one in `start` (default: the current folder)."""
    return find_env_file(start) or (start or pathlib.Path.cwd()).resolve() / ENV_FILE_NAME


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


def save(env_file: pathlib.Path, updates: Mapping[str, str]) -> None:
    """Write `updates` into `env_file`, replacing their lines and keeping every other line.

    Saves to the same file take turns (see `locked`), and each one writes a temporary file in the
    same folder that then replaces `env_file`: a reader sees the old file or the new one, never
    half of one, and a save that fails leaves the old file as it was. On POSIX the file is left
    readable and writable by its owner only (0600).
    """
    for name, value in updates.items():
        if name not in _BY_NAME:
            msg = f"Unknown environment variable: {name}"
            raise ValueError(msg)
        if any(unicodedata.category(char) in UNSAFE_CATEGORIES for char in value):
            msg = f"Invalid value for {name}: it holds a line break or another control character"
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
        _write(path, "\n".join(kept) + "\n")


def lock_file(env_file: pathlib.Path) -> pathlib.Path:
    """The file saves to `env_file` take turns on: `.env.lock`, next to `.env`."""
    path = env_file.resolve()
    return path.with_name(path.name + LOCK_SUFFIX)


@contextlib.contextmanager
def locked(env_file: pathlib.Path) -> Iterator[None]:
    """Hold the lock of `env_file`, waiting up to LOCK_TIMEOUT seconds for whoever holds it now.

    The lock is the operating system's, taken on `lock_file(env_file)`: it ends with the process
    that holds it, so a save that crashes leaves no stale lock (the empty file stays and is
    reused). Raises TimeoutError when the wait runs out.
    """
    descriptor = os.open(lock_file(env_file), os.O_RDWR | os.O_CREAT, FILE_MODE)
    try:
        deadline = time.monotonic() + LOCK_TIMEOUT
        while not _try_lock(descriptor):
            if time.monotonic() >= deadline:
                msg = f"{env_file} is being saved by another program; try again"
                raise TimeoutError(msg)
            time.sleep(LOCK_POLL)
        try:
            yield
        finally:
            _unlock(descriptor)
    finally:
        os.close(descriptor)


if sys.platform == "win32":

    def _try_lock(descriptor: int) -> bool:
        os.lseek(descriptor, 0, os.SEEK_SET)
        try:
            msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            if exc.errno in (errno.EACCES, errno.EDEADLOCK):  # someone else holds it
                return False
            raise
        return True

    def _unlock(descriptor: int) -> None:
        os.lseek(descriptor, 0, os.SEEK_SET)
        msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)

else:

    def _try_lock(descriptor: int) -> bool:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:  # someone else holds it
            return False
        return True

    def _unlock(descriptor: int) -> None:
        fcntl.flock(descriptor, fcntl.LOCK_UN)


def _write(path: pathlib.Path, text: str) -> None:
    """Put `text` in `path` through a temporary file in the same folder that then replaces it.

    `mkstemp` creates the temporary file readable and writable by its owner only, so on POSIX
    `path` ends up 0600 whatever its mode was before.
    """
    descriptor, name = tempfile.mkstemp(dir=path.parent, prefix=f"{path.name}.", suffix=".tmp")
    temporary = pathlib.Path(name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(text.encode("utf-8"))
            handle.flush()
            os.fsync(handle.fileno())
        _replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)  # still there only when the replacement failed


def _replace(temporary: pathlib.Path, path: pathlib.Path) -> None:
    """Put the finished temporary file in place of `path`.

    On Windows the replacement is refused while another program has `path` open (something
    reading the keys): it is given a few seconds to finish before the error is raised.
    """
    for attempt in range(1, REPLACE_ATTEMPTS + 1):
        try:
            temporary.replace(path)
        except PermissionError:
            if attempt == REPLACE_ATTEMPTS:
                raise
            time.sleep(REPLACE_WAIT)
        else:
            return


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
