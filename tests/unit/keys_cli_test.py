import pathlib

import pytest
from click.testing import CliRunner

from data_pipeline import credentials, keys_check
from data_pipeline.cli import cli
from data_pipeline.keys_check import Check, Verdict


@pytest.fixture(autouse=True)
def no_real_credentials(monkeypatch):
    for name in credentials.NAMES:
        monkeypatch.delenv(name, raising=False)


def checker(verdicts):
    """A stand-in for keys_check.check that answers from a dict: value -> verdict."""

    def fake(name, value, **_kwargs):
        return Check(name, verdicts.get(value, Verdict.OK), "fake")

    return fake


def test_keys_lists_every_key_with_its_origin_and_no_value(tmp_path):
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        pathlib.Path(".env").write_text("FRED_API_KEY=secret123\n", encoding="utf-8")
        result = runner.invoke(cli, ["keys"])
    assert result.exit_code == 0, result.output
    assert "secret123" not in result.output
    assert result.output.isascii()
    fred_line = next(line for line in result.output.splitlines() if "FRED_API_KEY" in line)
    assert fred_line.startswith("[ok]")
    assert ".env" in fred_line
    bls_line = next(line for line in result.output.splitlines() if "BLS_API_KEY" in line)
    assert bls_line.startswith("[ ]")
    assert "missing" in bls_line


def test_setup_saves_a_checked_key(tmp_path, monkeypatch):
    monkeypatch.setattr(keys_check, "check", checker({}))
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(cli, ["setup"], input="1\nfredvalue\n")
        content = pathlib.Path(".env").read_text(encoding="utf-8")
    assert result.exit_code == 0, result.output
    assert "fredvalue" not in result.output
    assert "[ok]  FRED_API_KEY" in result.output
    assert content == "FRED_API_KEY=fredvalue\n"


def test_setup_offers_to_retype_a_rejected_key(tmp_path, monkeypatch):
    monkeypatch.setattr(keys_check, "check", checker({"wrong": Verdict.REJECTED}))
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(cli, ["setup"], input="1\nwrong\ny\nright\n")
        content = pathlib.Path(".env").read_text(encoding="utf-8")
    assert result.exit_code == 0, result.output
    assert "[x]   FRED_API_KEY" in result.output
    assert content == "FRED_API_KEY=right\n"


def test_setup_builds_the_sec_user_agent_from_a_name_and_an_email(tmp_path, monkeypatch):
    monkeypatch.setattr(keys_check, "check", checker({}))
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(cli, ["setup"], input="6\nAna Lopez\nana@example.com\n")
        content = pathlib.Path(".env").read_text(encoding="utf-8")
    assert result.exit_code == 0, result.output
    assert content == "SEC_EDGAR_UA=Ana Lopez ana@example.com\n"


def test_setup_asks_before_replacing_and_no_check_skips_the_check(tmp_path, monkeypatch):
    def refuse(*_args, **_kwargs):
        msg = "no check expected"
        raise AssertionError(msg)

    monkeypatch.setattr(keys_check, "check", refuse)
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        pathlib.Path(".env").write_text("# mine\nFRED_API_KEY=old\n", encoding="utf-8")
        kept = runner.invoke(cli, ["setup", "--no-check"], input="1\nn\n")
        assert pathlib.Path(".env").read_text(encoding="utf-8") == "# mine\nFRED_API_KEY=old\n"
        replaced = runner.invoke(cli, ["setup", "--no-check"], input="1\ny\nnew\n")
        content = pathlib.Path(".env").read_text(encoding="utf-8")
    assert kept.exit_code == 0
    assert replaced.exit_code == 0
    assert content == "# mine\nFRED_API_KEY=new\n"


def test_setup_never_writes_to_an_env_file_above_the_project(tmp_path, monkeypatch):
    monkeypatch.setattr(keys_check, "check", checker({}))
    someone_elses = tmp_path / ".env"
    someone_elses.write_text("OTHER_TOOL_TOKEN=x\n", encoding="utf-8")
    root = tmp_path / "project"
    (root / ".git").mkdir(parents=True)
    monkeypatch.chdir(root)
    result = CliRunner().invoke(cli, ["setup"], input="1\nfredvalue\n")
    assert result.exit_code == 0, result.output
    assert someone_elses.read_text(encoding="utf-8") == "OTHER_TOOL_TOKEN=x\n"
    assert (root / ".env").read_text(encoding="utf-8") == "FRED_API_KEY=fredvalue\n"
    assert f"Keys are saved in {root.resolve() / '.env'}" in result.output


def test_setup_asks_before_writing_to_the_env_file_of_a_parent_folder(tmp_path, monkeypatch):
    monkeypatch.setattr(keys_check, "check", checker({}))
    root = tmp_path / "project"
    (root / "notebooks").mkdir(parents=True)
    (root / "pyproject.toml").write_text("", encoding="utf-8")
    (root / ".env").write_text("# mine\n", encoding="utf-8")
    monkeypatch.chdir(root / "notebooks")
    runner = CliRunner()
    declined = runner.invoke(cli, ["setup"], input="n\n")
    assert declined.exit_code == 0, declined.output
    assert "That .env is in a parent folder. Save the keys there?" in declined.output
    assert "Nothing saved." in declined.output
    assert (root / ".env").read_text(encoding="utf-8") == "# mine\n"
    accepted = runner.invoke(cli, ["setup"], input="y\n1\nfred\n")
    assert accepted.exit_code == 0, accepted.output
    assert (root / ".env").read_text(encoding="utf-8") == "# mine\nFRED_API_KEY=fred\n"
    assert not (root / "notebooks" / ".env").exists()


def test_setup_refuses_numbers_outside_the_list(tmp_path):
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(cli, ["setup"], input="12\n")
    assert result.exit_code == 2
    assert "1 to 6" in result.output


def test_setup_enter_picks_every_missing_key_and_enter_skips_each_one(tmp_path):
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(cli, ["setup"], input="\n" * 7)
        written = pathlib.Path(".env").exists()
    assert result.exit_code == 0, result.output
    assert "Nothing saved." in result.output
    assert not written


def test_setup_keeps_the_keys_accepted_before_an_interruption(tmp_path, monkeypatch):
    monkeypatch.setattr(keys_check, "check", checker({}))
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(cli, ["setup"], input="1,2\nfred\n")
        content = pathlib.Path(".env").read_text(encoding="utf-8")
    assert result.exit_code != 0
    assert content == "FRED_API_KEY=fred\n"


def test_setup_gives_up_on_a_rejected_key_and_still_saves_the_next(tmp_path, monkeypatch):
    monkeypatch.setattr(keys_check, "check", checker({"wrong": Verdict.REJECTED}))
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(cli, ["setup"], input="1,2\nwrong\nn\nbls\n")
        content = pathlib.Path(".env").read_text(encoding="utf-8")
    assert result.exit_code == 0, result.output
    assert content == "BLS_API_KEY=bls\n"


def test_setup_saves_a_key_whose_check_could_not_tell(tmp_path, monkeypatch):
    monkeypatch.setattr(keys_check, "check", checker({"maybe": Verdict.UNKNOWN}))
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(cli, ["setup"], input="1\nmaybe\n")
        content = pathlib.Path(".env").read_text(encoding="utf-8")
    assert result.exit_code == 0, result.output
    assert "[?]   FRED_API_KEY" in result.output
    assert content == "FRED_API_KEY=maybe\n"


def test_setup_asks_before_saving_a_key_the_environment_already_provides(tmp_path, monkeypatch):
    monkeypatch.setenv("FRED_API_KEY", "fromenv")
    monkeypatch.setattr(keys_check, "check", checker({}))
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        declined = runner.invoke(cli, ["setup"], input="1\nn\n")
        written = pathlib.Path(".env").exists()
        accepted = runner.invoke(cli, ["setup"], input="1\ny\nfromfile\n")
        content = pathlib.Path(".env").read_text(encoding="utf-8")
    assert declined.exit_code == 0, declined.output
    assert "set in the environment, which wins over .env. Save one in .env anyway?" in declined.output
    assert not written
    assert accepted.exit_code == 0, accepted.output
    assert content == "FRED_API_KEY=fromfile\n"


def test_setup_asks_again_for_an_sec_contact_without_an_email_even_with_no_check(tmp_path):
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(
            cli,
            ["setup", "--no-check"],
            input="6\nAna Lopez\nnot-an-email\ny\nAna Lopez\nana@example.com\n",
        )
        content = pathlib.Path(".env").read_text(encoding="utf-8")
    assert result.exit_code == 0, result.output
    assert "[x]   SEC_EDGAR_UA  the SEC asks for a contact e-mail" in result.output
    assert content == "SEC_EDGAR_UA=Ana Lopez ana@example.com\n"
