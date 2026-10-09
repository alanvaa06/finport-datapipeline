import errno
import os
import sys

import pytest

from data_pipeline import _files
from data_pipeline._files import ReplaceRefusedError, try_lock, unlock, write_atomic


def opened(path):
    return os.open(path, os.O_RDWR | os.O_CREAT, 0o644)


def test_a_lock_is_taken_once_until_it_is_released(tmp_path):
    path = tmp_path / "x.lock"
    first, second = opened(path), opened(path)
    try:
        assert try_lock(first)
        assert not try_lock(second)
        unlock(first)
        assert try_lock(second)
        unlock(second)
    finally:
        os.close(first)
        os.close(second)


def test_the_content_of_a_locked_file_stays_readable(tmp_path):
    path = tmp_path / "x.lock"
    descriptor = opened(path)
    try:
        assert try_lock(descriptor)
        os.write(descriptor, b'{"pid": 1}')
        assert path.read_bytes() == b'{"pid": 1}'
    finally:
        unlock(descriptor)
        os.close(descriptor)


def test_an_error_that_is_not_another_holder_is_raised(tmp_path):
    descriptor = os.open(tmp_path / "x.lock", os.O_RDWR | os.O_CREAT, 0o644)
    os.close(descriptor)
    with pytest.raises(OSError):  # a closed descriptor: EBADF, not "someone else holds it"
        try_lock(descriptor)


def test_a_write_replaces_the_file_and_leaves_no_temporary_file(tmp_path):
    path = tmp_path / "data.json"
    path.write_bytes(b"old")
    write_atomic(path, b"new")
    assert path.read_bytes() == b"new"
    assert sorted(item.name for item in tmp_path.iterdir()) == ["data.json"]


def test_the_content_reaches_the_disk_before_it_replaces_the_file(tmp_path, monkeypatch):
    events = []
    real_fsync, real_replace = os.fsync, os.replace
    monkeypatch.setattr(_files.os, "fsync", lambda descriptor: events.append("fsync") or real_fsync(descriptor))
    monkeypatch.setattr(
        _files.os, "replace", lambda source, target: events.append("replace") or real_replace(source, target)
    )
    write_atomic(tmp_path / "data.json", b"new")
    assert events[:2] == ["fsync", "replace"]


def test_the_folder_is_flushed_once_the_file_is_replaced(tmp_path, monkeypatch):
    seen = []
    path = tmp_path / "data.json"
    monkeypatch.setattr(_files, "_sync_folder", lambda folder: seen.append((folder, path.read_bytes())))
    write_atomic(path, b"new")
    assert seen == [(tmp_path, b"new")]  # after the replacement: the rename is what it makes durable


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX flushes a folder through a descriptor")
def test_on_posix_the_folder_is_fsynced(tmp_path, monkeypatch):
    synced = []
    real = os.fsync
    monkeypatch.setattr(_files.os, "fsync", lambda descriptor: synced.append(descriptor) or real(descriptor))
    _files._sync_folder(tmp_path)
    assert len(synced) == 1


@pytest.mark.skipif(sys.platform != "win32", reason="Windows cannot open a folder with os.open")
def test_on_windows_the_folder_is_left_alone(tmp_path, monkeypatch):
    monkeypatch.setattr(_files.os, "open", lambda *_args: pytest.fail("a folder was opened"))
    _files._sync_folder(tmp_path)


def test_a_folder_that_cannot_be_flushed_does_not_fail_a_write_that_succeeded(tmp_path, monkeypatch):
    def unsupported(_descriptor):
        raise OSError(errno.EINVAL, "Invalid argument")

    monkeypatch.setattr(_files.sys, "platform", "linux")
    monkeypatch.setattr(_files.os, "open", lambda *_args: 99)
    monkeypatch.setattr(_files.os, "fsync", unsupported)
    monkeypatch.setattr(_files.os, "close", lambda _descriptor: None)
    _files._sync_folder(tmp_path)  # some network file systems refuse to flush a folder


def test_a_target_held_open_is_retried_and_then_replaced(tmp_path, monkeypatch):
    path = tmp_path / "data.json"
    real = os.replace
    attempts, waits = [], []

    def busy_twice(source, target):
        attempts.append(target)
        if len(attempts) <= 2:
            raise PermissionError(13, "the file is being used by another process")
        return real(source, target)

    monkeypatch.setattr(_files.os, "replace", busy_twice)
    monkeypatch.setattr(_files, "_sleep", waits.append)
    write_atomic(path, b"new")
    assert (len(attempts), waits) == (3, [_files.REPLACE_WAIT] * 2)
    assert path.read_bytes() == b"new"


def test_a_target_that_stays_open_is_refused_and_keeps_its_content(tmp_path, monkeypatch):
    path = tmp_path / "data.json"
    path.write_bytes(b"old")

    def always_busy(_source, _target):
        raise PermissionError(13, "the file is being used by another process")

    monkeypatch.setattr(_files.os, "replace", always_busy)
    monkeypatch.setattr(_files, "_sleep", lambda _seconds: None)
    with pytest.raises(ReplaceRefusedError):
        write_atomic(path, b"new")
    monkeypatch.undo()
    assert path.read_bytes() == b"old"
    assert sorted(item.name for item in tmp_path.iterdir()) == ["data.json"]


@pytest.mark.skipif(sys.platform == "win32", reason="file modes are POSIX")
def test_the_mode_given_is_the_mode_of_the_new_file(tmp_path):
    path = tmp_path / ".env"
    write_atomic(path, b"KEY=1\n", mode=0o600)
    assert path.stat().st_mode & 0o777 == 0o600
