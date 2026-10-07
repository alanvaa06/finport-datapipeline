import os
import pathlib

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


def test_the_nearest_env_file_is_found_from_a_subfolder(tmp_path):
    (tmp_path / ".env").write_text("FRED_API_KEY=parent\n", encoding="utf-8")
    notebooks = tmp_path / "notebooks" / "deep"
    notebooks.mkdir(parents=True)
    assert find_env_file(notebooks) == tmp_path / ".env"
    assert resolve(environ={}, start=notebooks).get("FRED_API_KEY") == "parent"


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
    from tests.conftest import confine_env_search

    bound = tmp_path / "bound"
    outside = tmp_path / "outside"
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
