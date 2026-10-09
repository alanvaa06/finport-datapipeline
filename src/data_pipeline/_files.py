"""Files that more than one process may touch: the operating system's lock, and an atomic write.

Both the store (`data_pipeline.store`: `sync.lock` and every parquet and JSON file it writes) and
the credentials (`data_pipeline.credentials`: `.env` and the lock its saves take turns on) use
this module, so the two take a lock and replace a file the same way. It imports nothing from
either of them.

The lock is the operating system's, so it ends with the process that holds it, however that
process ends: flock on the whole file on POSIX; on Windows, a byte-range lock (msvcrt) on one
byte far past any content, so the file's own content stays readable while it is held.

An atomic write puts the content in a new temporary file in the same folder, flushes it to the
disk and then replaces the target with it: a reader sees the old file or the new one, never half
of one, and a write that fails leaves the old file as it was. Windows refuses the replacement
while another program has the target open; it is tried again for about five seconds. On POSIX
the folder is flushed after the replacement, so the replacement itself survives a power cut.
"""

import contextlib
import errno
import os
import pathlib
import secrets
import sys
import time

if sys.platform == "win32":
    import msvcrt

    BINARY = os.O_BINARY  # bytes as they are: no newline translation
else:
    import fcntl

    BINARY = 0

LOCKED_BYTE = 1 << 30  # the byte Windows locks: far past the content of any lock file
REPLACE_ATTEMPTS = 10
REPLACE_WAIT = 0.5  # seconds between attempts: about five seconds in all
# What the system answers when another holder has the lock (any other error is raised)
HELD = frozenset({errno.EACCES, errno.EDEADLOCK} if sys.platform == "win32" else {errno.EAGAIN, errno.EWOULDBLOCK})
_sleep = time.sleep


class ReplaceRefusedError(PermissionError):
    """The target stayed open in another program for every attempt to replace it (Windows)."""


def try_lock(descriptor: int) -> bool:
    """Take the lock of an open file without waiting: True when taken, False when another
    holder has it. Any other failure (a closed descriptor, say) is raised. The descriptor's
    position is left where it was."""
    try:
        if sys.platform == "win32":
            _windows_lock(descriptor, msvcrt.LK_NBLCK)
        else:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        if exc.errno in HELD:
            return False
        raise
    return True


def unlock(descriptor: int) -> None:
    """Release the lock `try_lock` took on `descriptor`. The descriptor stays open."""
    if sys.platform == "win32":
        _windows_lock(descriptor, msvcrt.LK_UNLCK)
    else:
        fcntl.flock(descriptor, fcntl.LOCK_UN)


def _windows_lock(descriptor: int, operation: int) -> None:
    """Lock or unlock LOCKED_BYTE: msvcrt acts at the current position, which is then restored."""
    if sys.platform == "win32":
        position = os.lseek(descriptor, 0, os.SEEK_CUR)
        os.lseek(descriptor, LOCKED_BYTE, os.SEEK_SET)
        try:
            msvcrt.locking(descriptor, operation, 1)
        finally:
            os.lseek(descriptor, position, os.SEEK_SET)


def _replace(temporary: pathlib.Path, path: pathlib.Path) -> None:
    """Put the finished temporary file in place of `path`, retrying while Windows refuses because
    another program has `path` open. Raises ReplaceRefusedError when it never stops refusing."""
    for attempt in range(1, REPLACE_ATTEMPTS + 1):
        try:
            temporary.replace(path)
        except PermissionError as exc:
            if attempt == REPLACE_ATTEMPTS:
                raise ReplaceRefusedError(*exc.args) from exc
            _sleep(REPLACE_WAIT)
        else:
            return


def write_atomic(path: pathlib.Path, content: bytes, *, mode: int = 0o666) -> None:
    """Replace `path` with `content` through a temporary file in the same folder, flushed to the
    disk first: without the flush, a power cut after the replacement can leave a torn file.

    The temporary file is new (its name is random), so it never reuses one left by a write that
    died; it is removed when the replacement fails. `mode` is the new file's permissions on
    POSIX, before the umask: the credentials pass 0o600. The folder must exist.
    """
    temporary = path.with_name(f"{path.name}.{secrets.token_hex(4)}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | BINARY
    descriptor = os.open(temporary, flags, mode)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        _replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)  # still there only when the replacement failed
    _sync_folder(path.parent)


def _sync_folder(folder: pathlib.Path) -> None:
    """Flush a folder's entries to the disk on POSIX, so a file just replaced in it stays
    replaced after a power cut: the rename lives in the folder, not in the file. Windows cannot
    open a folder with os.open, so it is skipped there. A file system that refuses to flush a
    folder (some network mounts) does not fail a write that already took place."""
    if sys.platform == "win32":
        return
    with contextlib.suppress(OSError):
        descriptor = os.open(folder, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
