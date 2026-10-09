import os
import pathlib
import stat
import sys
import threading
import time

import pytest

from data_pipeline import credentials
from data_pipeline.credentials import Credentials, find_env_file, resolve, save, target_env_file


@pytest.fixture(autouse=True)
def no_real_credentials(monkeypatch):
    for name in credentials.NAMES:
        monkeypatch.delenv(name, raising=False)


def test_the_registry_holds_the_six_keys_in_setup_order():
    assert credentials.NAMES == (
        "FRED_API_KEY",
        "BLS_API_KEY",
        "BANXICO_TOKEN",
        "INEGI_TOKEN",
        "COMTRADE_API_KEY",
        "SEC_EDGAR_UA",
    )


def test_every_registry_text_is_ascii():
    for key in credentials.REGISTRY:
        for text in (key.label, key.scope, key.how):
            assert text.isascii(), key.name


def test_code_wins_over_environment_and_environment_over_file(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("FRED_API_KEY=file\nBLS_API_KEY=file\nINEGI_TOKEN=file\n", encoding="utf-8")
    resolved = resolve(
        {"FRED_API_KEY": "code"},
        environ={"FRED_API_KEY": "env", "BLS_API_KEY": "env"},
        env_file=env_file,
    )
    assert (resolved.get("FRED_API_KEY"), resolved.origin("FRED_API_KEY")) == ("code", "code")
    assert (resolved.get("BLS_API_KEY"), resolved.origin("BLS_API_KEY")) == ("env", "environment")
    assert (resolved.get("INEGI_TOKEN"), resolved.origin("INEGI_TOKEN")) == ("file", str(env_file))
    assert (resolved.get("BANXICO_TOKEN"), resolved.origin("BANXICO_TOKEN")) == (None, None)


def test_blank_values_count_as_absent(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("FRED_API_KEY=\n", encoding="utf-8")
    resolved = resolve({"BLS_API_KEY": " "}, environ={"INEGI_TOKEN": "   "}, env_file=env_file)
    assert resolved.values == {}


def test_a_missing_file_is_not_an_error(tmp_path):
    assert resolve(environ={}, env_file=tmp_path / "absent.env").values == {}


def test_an_unknown_name_in_code_is_refused():
    with pytest.raises(ValueError, match="Unknown credential: FRED_KEY"):
        resolve({"FRED_KEY": "x"}, environ={})


def project(folder: pathlib.Path, marker: str = ".git") -> pathlib.Path:
    """Make `folder` the root of a project: a git clone (a `.git` folder) unless `marker` says otherwise."""
    folder.mkdir(parents=True, exist_ok=True)
    if marker == ".git":
        (folder / marker).mkdir()
    else:
        (folder / marker).write_text("", encoding="utf-8")
    return folder


def test_the_nearest_env_file_is_found_from_a_subfolder(tmp_path):
    project(tmp_path)
    (tmp_path / ".env").write_text("FRED_API_KEY=parent\n", encoding="utf-8")
    notebooks = tmp_path / "notebooks" / "deep"
    notebooks.mkdir(parents=True)
    assert find_env_file(notebooks) == tmp_path / ".env"
    assert resolve(environ={}, start=notebooks).get("FRED_API_KEY") == "parent"


@pytest.mark.parametrize("marker", [".git", "pyproject.toml", ".git file"])
def test_the_search_stops_at_the_root_of_the_project(tmp_path, marker):
    (tmp_path / ".env").write_text("FRED_API_KEY=someone_elses\n", encoding="utf-8")
    root = tmp_path / "project"
    if marker == ".git file":  # a git worktree or submodule has a `.git` file, not a folder
        root.mkdir()
        (root / ".git").write_text("gitdir: elsewhere\n", encoding="utf-8")
    else:
        project(root, marker)
    notebooks = root / "notebooks"
    notebooks.mkdir()
    assert find_env_file(notebooks) is None
    assert target_env_file(notebooks) == notebooks.resolve() / ".env"
    assert resolve(environ={}, start=notebooks).get("FRED_API_KEY") is None


def test_outside_a_project_only_the_folder_itself_is_searched(tmp_path):
    (tmp_path / ".env").write_text("FRED_API_KEY=shared\n", encoding="utf-8")  # a shared /tmp/.env, a synced folder's
    work = tmp_path / "work"
    work.mkdir()
    assert find_env_file(work) is None
    (work / ".env").write_text("FRED_API_KEY=mine\n", encoding="utf-8")
    assert find_env_file(work) == work / ".env"


def test_the_home_folder_is_never_searched_from_below(tmp_path, monkeypatch):
    home = project(tmp_path / "home")  # a home kept in git, as dotfiles often are
    (home / ".env").write_text("FRED_API_KEY=home\n", encoding="utf-8")
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))
    analysis = home / "analysis"
    analysis.mkdir()
    assert find_env_file(analysis) is None
    assert find_env_file(home) == home / ".env"  # from the home folder itself, its own .env counts


def test_the_closest_env_file_wins(tmp_path):
    (tmp_path / ".env").write_text("FRED_API_KEY=parent\n", encoding="utf-8")
    child = tmp_path / "child"
    child.mkdir()
    (child / ".env").write_text("FRED_API_KEY=child\n", encoding="utf-8")
    assert find_env_file(child) == child / ".env"


def test_no_env_file_anywhere_returns_none(tmp_path, monkeypatch):
    monkeypatch.setattr(pathlib.Path, "is_file", lambda self: False)
    assert find_env_file(tmp_path) is None
    assert target_env_file(tmp_path) == tmp_path.resolve() / ".env"


def test_repr_never_shows_a_value():
    text = repr(Credentials({"FRED_API_KEY": "abc123"}))
    assert "abc123" not in text
    assert "FRED_API_KEY=yes" in text
    assert "BLS_API_KEY=no" in text


def test_the_sec_user_agent_is_not_a_secret():
    resolved = Credentials({"FRED_API_KEY": "abc123", "SEC_EDGAR_UA": "Name name@example.com"})
    assert resolved.secrets() == ("abc123",)


def test_save_replaces_appends_and_keeps_other_lines(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("# comment\nBLS_API_KEY=\nOTHER_VAR=keep\n", encoding="utf-8")
    save(env_file, {"BLS_API_KEY": "new", "FRED_API_KEY": "fred"})
    assert env_file.read_text(encoding="utf-8") == "# comment\nBLS_API_KEY=new\nOTHER_VAR=keep\nFRED_API_KEY=fred\n"


def test_save_creates_the_file(tmp_path):
    save(tmp_path / ".env", {"INEGI_TOKEN": "inegi"})
    assert (tmp_path / ".env").read_text(encoding="utf-8") == "INEGI_TOKEN=inegi\n"


def test_save_refuses_unknown_names_and_line_breaks(tmp_path):
    with pytest.raises(ValueError, match="Unknown environment variable: EVIL_VAR"):
        save(tmp_path / ".env", {"EVIL_VAR": "x"})
    with pytest.raises(ValueError, match="Invalid value for FRED_API_KEY"):
        save(tmp_path / ".env", {"FRED_API_KEY": "a\nb"})
    assert not (tmp_path / ".env").exists()


@pytest.mark.parametrize("character", ["\u2028", "\u2029", "\x85", "\x0b", "\x0c", "\x1c", "\x00", "\t"])
def test_save_refuses_line_separators_and_control_characters(tmp_path, character):
    env_file = tmp_path / ".env"
    with pytest.raises(ValueError, match="Invalid value for FRED_API_KEY"):
        save(env_file, {"FRED_API_KEY": f"abc{character}BLS_API_KEY=planted"})
    assert not env_file.exists()


def test_a_line_separator_inside_a_line_never_splits_it_on_save(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("NOTE=a\u2028FRED_API_KEY=planted\nBLS_API_KEY=old\n", encoding="utf-8")
    assert resolve(environ={}, env_file=env_file).get("FRED_API_KEY") is None  # python-dotenv reads one line
    save(env_file, {"BLS_API_KEY": "new"})
    assert env_file.read_text(encoding="utf-8") == "NOTE=a\u2028FRED_API_KEY=planted\nBLS_API_KEY=new\n"
    assert resolve(environ={}, env_file=env_file).get("FRED_API_KEY") is None


def test_save_reads_every_kind_of_line_ending(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_bytes(b"A=1\r\nBLS_API_KEY=old\rB=2\n")
    save(env_file, {"BLS_API_KEY": "new"})
    assert env_file.read_bytes() == b"A=1\nBLS_API_KEY=new\nB=2\n"


def test_two_saves_at_once_keep_both_keys(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("# keep me\nOTHER_VAR=keep\n", encoding="utf-8")
    read_text = pathlib.Path.read_text

    def slow_read_text(self, *args, **kwargs):  # widens each save's read-modify-write window
        text = read_text(self, *args, **kwargs)
        time.sleep(0.3)
        return text

    monkeypatch.setattr(pathlib.Path, "read_text", slow_read_text)
    errors: list[BaseException] = []

    def writer(name):
        try:
            save(env_file, {name: "v"})
        except BaseException as exc:  # noqa: BLE001  # reported by the assertion below
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(name,)) for name in ("FRED_API_KEY", "BLS_API_KEY")]
    for thread in threads:
        thread.start()
        time.sleep(0.05)
    for thread in threads:
        thread.join()
    monkeypatch.undo()
    assert errors == []
    assert resolve(environ={}, env_file=env_file).values == {"FRED_API_KEY": "v", "BLS_API_KEY": "v"}
    assert env_file.read_text(encoding="utf-8").startswith("# keep me\nOTHER_VAR=keep\n")


def test_a_save_that_fails_leaves_the_file_as_it_was(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("# mine\nFRED_API_KEY=old\n", encoding="utf-8")

    def full_disk(*_args, **_kwargs):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(os, "replace", full_disk)
    with pytest.raises(OSError, match="No space left on device"):
        save(env_file, {"FRED_API_KEY": "new"})
    monkeypatch.undo()
    assert env_file.read_text(encoding="utf-8") == "# mine\nFRED_API_KEY=old\n"
    assert not [path.name for path in tmp_path.iterdir() if path.name.endswith(".tmp")]


def test_a_save_gives_up_when_another_one_holds_the_file(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    monkeypatch.setattr(credentials, "LOCK_TIMEOUT", 0.2)
    with credentials.locked(env_file), pytest.raises(TimeoutError, match="being saved by another program"):
        save(env_file, {"FRED_API_KEY": "x"})
    save(env_file, {"FRED_API_KEY": "x"})  # once released, the next save goes through
    assert env_file.read_text(encoding="utf-8") == "FRED_API_KEY=x\n"


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX permissions")
def test_the_saved_file_is_readable_by_its_owner_only(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("OTHER_VAR=keep\n", encoding="utf-8")
    env_file.chmod(0o644)
    save(env_file, {"FRED_API_KEY": "x"})
    assert stat.S_IMODE(env_file.stat().st_mode) == 0o600
    save(tmp_path / "new.env", {"FRED_API_KEY": "x"})
    assert stat.S_IMODE((tmp_path / "new.env").stat().st_mode) == 0o600


def test_a_save_through_a_symbolic_link_writes_the_file_it_points_at(tmp_path):
    real = tmp_path / "keys" / "project.env"
    real.parent.mkdir()
    real.write_text("OTHER_VAR=keep\n", encoding="utf-8")
    link = tmp_path / ".env"
    try:
        link.symlink_to(real)
    except OSError:
        pytest.skip("this system does not let the user create symbolic links")
    save(link, {"FRED_API_KEY": "x"})
    assert link.is_symlink()
    assert real.read_text(encoding="utf-8") == "OTHER_VAR=keep\nFRED_API_KEY=x\n"


def test_the_missing_message_names_the_key_where_to_get_it_and_the_command():
    assert credentials.missing_message("FRED_API_KEY") == (
        "FRED_API_KEY is missing. Get one at https://fredaccount.stlouisfed.org/apikey and run: data-pipeline setup"
    )


def test_save_replaces_every_line_of_a_key(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("FRED_API_KEY=a\nOTHER_VAR=keep\nFRED_API_KEY=b\n", encoding="utf-8")
    save(env_file, {"FRED_API_KEY": "new"})
    assert env_file.read_text(encoding="utf-8") == "FRED_API_KEY=new\nOTHER_VAR=keep\n"
    assert resolve(environ={}, env_file=env_file).get("FRED_API_KEY") == "new"


def test_save_recognises_a_leading_export(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("export FRED_API_KEY=old\nOTHER_VAR=keep\n", encoding="utf-8")
    save(env_file, {"FRED_API_KEY": "new"})
    assert env_file.read_text(encoding="utf-8") == "FRED_API_KEY=new\nOTHER_VAR=keep\n"


@pytest.mark.parametrize(
    "value",
    ["ab#cd", "ab$HOME", "it's", 'a"b\\c', " padded ", "a\\nb", "Ana Lopez ana@example.com"],
)
def test_values_read_back_exactly_as_written(tmp_path, value):
    env_file = tmp_path / ".env"
    save(env_file, {"FRED_API_KEY": value})
    assert resolve(environ={}, env_file=env_file).get("FRED_API_KEY") == value.strip()


def test_only_values_that_need_it_are_quoted(tmp_path):
    env_file = tmp_path / ".env"
    save(env_file, {"FRED_API_KEY": "abc", "SEC_EDGAR_UA": "Ana Lopez ana@example.com", "BLS_API_KEY": "ab#cd"})
    assert env_file.read_text(encoding="utf-8") == (
        'FRED_API_KEY=abc\nSEC_EDGAR_UA=Ana Lopez ana@example.com\nBLS_API_KEY="ab#cd"\n'
    )


def test_a_file_with_a_utf8_bom_still_resolves_and_saves_its_first_key(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_bytes(b"\xef\xbb\xbfFRED_API_KEY=old\n")
    assert resolve(environ={}, env_file=env_file).get("FRED_API_KEY") == "old"
    save(env_file, {"FRED_API_KEY": "new"})
    assert env_file.read_bytes() == b"FRED_API_KEY=new\n"


def test_the_origin_of_a_file_value_is_the_absolute_path(tmp_path, monkeypatch):
    (tmp_path / ".env").write_text("FRED_API_KEY=file\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    resolved = resolve(environ={}, env_file=pathlib.Path(".env"))
    assert resolved.origin("FRED_API_KEY") == str((tmp_path / ".env").resolve())


def test_an_explicit_env_file_wins_over_the_search_start(tmp_path):
    here = tmp_path / "here"
    here.mkdir()
    (here / ".env").write_text("FRED_API_KEY=searched\n", encoding="utf-8")
    chosen = tmp_path / "chosen.env"
    chosen.write_text("FRED_API_KEY=chosen\n", encoding="utf-8")
    assert resolve(environ={}, env_file=chosen, start=here).get("FRED_API_KEY") == "chosen"


def test_a_test_never_finds_an_env_file_outside_its_temp_folder(tmp_path):
    from tests.unit.conftest import confine_env_search

    bound = project(tmp_path / "bound")
    outside = project(tmp_path / "outside")
    (bound / "work").mkdir(parents=True)
    (outside / "work").mkdir(parents=True)
    (outside / ".env").write_text("FRED_API_KEY=real\n", encoding="utf-8")
    (bound / ".env").write_text("FRED_API_KEY=test\n", encoding="utf-8")
    confined = confine_env_search(find_env_file, bound)
    assert find_env_file(outside / "work") == (outside / ".env").resolve()  # the real search does find it
    assert confined(outside / "work") is None
    assert confined(bound / "work") == (bound / ".env").resolve()


def test_the_autouse_fixture_confines_the_module_search_and_clears_the_keys(monkeypatch):
    assert credentials.find_env_file is not find_env_file  # the module attribute is the confined wrapper
    assert not any(name in os.environ for name in credentials.NAMES)
