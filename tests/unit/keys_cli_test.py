import pathlib
import shutil
import subprocess

import pytest
from click.testing import CliRunner

from data_pipeline import credentials, keys_check, keys_cli
from data_pipeline.cli import cli
from data_pipeline.keys_check import Check, Verdict

GIT = shutil.which("git")
needs_git = pytest.mark.skipif(GIT is None, reason="git is not installed")


@pytest.fixture(autouse=True)
def no_real_credentials(monkeypatch):
    for name in credentials.NAMES:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture(autouse=True)
def hermetic_git(monkeypatch, tmp_path_factory):
    """git sees only the repositories a test makes: no repository above the temp folder, no user or system config."""
    base = tmp_path_factory.getbasetemp()
    for name in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR", "GIT_OBJECT_DIRECTORY"):
        monkeypatch.delenv(name, raising=False)  # set when the tests run from a git hook
    empty_config = base / "empty.gitconfig"
    empty_config.touch()
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(base))
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(empty_config))
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(base / "no-xdg-config"))  # where git's default global ignore file lives


def git(folder, *args):
    assert GIT is not None
    subprocess.run([GIT, *args], cwd=folder, check=True, capture_output=True)


def repository(folder):
    """A real git repository at `folder`."""
    folder.mkdir(parents=True, exist_ok=True)
    git(folder, "init", "-q")
    return folder


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


def test_setup_from_a_subfolder_creates_the_env_file_at_the_project_root(tmp_path, monkeypatch):
    monkeypatch.setattr(keys_check, "check", checker({}))
    root = tmp_path / "project"
    (root / "notebooks").mkdir(parents=True)
    (root / "scripts").mkdir()
    (root / "pyproject.toml").write_text("", encoding="utf-8")
    monkeypatch.chdir(root / "notebooks")
    result = CliRunner().invoke(cli, ["setup"], input="y\n1\nfred\n")
    assert result.exit_code == 0, result.output
    assert f"Keys are saved in {root.resolve() / '.env'}" in result.output
    assert (root / ".env").read_text(encoding="utf-8") == "FRED_API_KEY=fred\n"
    assert not (root / "notebooks" / ".env").exists()
    monkeypatch.chdir(root / "scripts")  # another subfolder finds it
    listed = CliRunner().invoke(cli, ["keys"])
    assert next(line for line in listed.output.splitlines() if "FRED_API_KEY" in line).startswith("[ok]")


@needs_git
@pytest.mark.parametrize(
    ("before", "after"),
    [
        (None, b".env\n.env.lock\n"),
        (b"build/\r\n", b"build/\r\n.env\r\n.env.lock\r\n"),  # its line endings are kept
        (b"build/", b"build/\n.env\n.env.lock\n"),
        (b"\xef\xbb\xbf.env\n", b"\xef\xbb\xbf.env\n.env.lock\n"),  # already listed, behind a BOM
        (b"/.env\n/.env.lock\n", b"/.env\n/.env.lock\n"),
        (b".env*\n", b".env*\n"),  # a pattern covers both
        (b"**/.env\n", b"**/.env\n.env.lock\n"),
        (b".env\n.env.lock\n!.env.lock\n", b".env\n.env.lock\n!.env.lock\n"),  # an un-ignored lock file stays so
    ],
)
def test_setup_keeps_the_env_file_out_of_git(tmp_path, monkeypatch, before, after):
    monkeypatch.setattr(keys_check, "check", checker({}))
    root = repository(tmp_path / "project")
    if before is not None:
        (root / ".gitignore").write_bytes(before)
    monkeypatch.chdir(root)
    result = CliRunner().invoke(cli, ["setup"], input="1\nfred\n")
    assert result.exit_code == 0, result.output
    assert (root / ".gitignore").read_bytes() == after
    assert (root / ".env").read_text(encoding="utf-8") == "FRED_API_KEY=fred\n"


@needs_git
def test_setup_takes_the_ignore_rules_of_every_level_into_account(tmp_path, monkeypatch):
    monkeypatch.setattr(keys_check, "check", checker({}))
    root = repository(tmp_path / "project")
    (root / ".gitignore").write_bytes(b".env*\n")
    (root / "notebooks").mkdir()
    (root / "notebooks" / ".env").write_text("", encoding="utf-8")  # its own .env, still ignored from the root
    monkeypatch.chdir(root / "notebooks")
    result = CliRunner().invoke(cli, ["setup"], input="1\nfred\n")
    assert result.exit_code == 0, result.output
    assert not (root / "notebooks" / ".gitignore").exists()
    assert (root / ".gitignore").read_bytes() == b".env*\n"


@needs_git
@pytest.mark.parametrize("rules", [b"!.env\n", b".env\n!/.env\n"])
def test_setup_stops_when_a_rule_un_ignores_the_env_file(tmp_path, monkeypatch, rules):
    monkeypatch.setattr(keys_check, "check", checker({}))
    root = repository(tmp_path / "project")
    (root / ".gitignore").write_bytes(rules)
    monkeypatch.chdir(root)
    result = CliRunner().invoke(cli, ["setup"], input="1\nfred\n")
    assert result.exit_code == 1, result.output
    assert "un-ignores" in result.output
    assert ".gitignore" in result.output
    assert "Which ones?" not in result.output
    assert not (root / ".env").exists()
    assert (root / ".gitignore").read_bytes() == rules


@needs_git
def test_setup_outside_a_repository_writes_no_gitignore(tmp_path, monkeypatch):
    monkeypatch.setattr(keys_check, "check", checker({}))
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(cli, ["setup"], input="1\nfred\n")
    assert result.exit_code == 0, result.output
    assert (tmp_path / ".env").read_text(encoding="utf-8") == "FRED_API_KEY=fred\n"
    assert not (tmp_path / ".gitignore").exists()


@pytest.mark.parametrize(
    ("before", "after"),
    [
        (None, b".env\n.env.lock\n"),
        (b"/.env\n", b"/.env\n.env.lock\n"),
    ],
)
def test_setup_without_git_still_lists_the_env_file_in_a_git_project(tmp_path, monkeypatch, before, after):
    monkeypatch.setattr(keys_check, "check", checker({}))
    monkeypatch.setattr(keys_cli, "GIT", "no-such-git-executable")
    root = tmp_path / "project"
    (root / ".git").mkdir(parents=True)  # a clone made with a git that is not on PATH
    if before is not None:
        (root / ".gitignore").write_bytes(before)
    monkeypatch.chdir(root)
    result = CliRunner().invoke(cli, ["setup"], input="1\nfred\n")
    assert result.exit_code == 0, result.output
    assert (root / ".gitignore").read_bytes() == after


def test_setup_without_git_stops_at_an_un_ignored_env_file(tmp_path, monkeypatch):
    monkeypatch.setattr(keys_check, "check", checker({}))
    monkeypatch.setattr(keys_cli, "GIT", "no-such-git-executable")
    root = tmp_path / "project"
    (root / ".git").mkdir(parents=True)
    (root / ".gitignore").write_bytes(b"*.env\n!/.env\n")
    monkeypatch.chdir(root)
    result = CliRunner().invoke(cli, ["setup"], input="1\nfred\n")
    assert result.exit_code == 1, result.output
    assert not (root / ".env").exists()


def test_setup_without_git_writes_no_gitignore_outside_a_git_project(tmp_path, monkeypatch):
    monkeypatch.setattr(keys_check, "check", checker({}))
    monkeypatch.setattr(keys_cli, "GIT", "no-such-git-executable")
    root = tmp_path / "project"
    root.mkdir()
    (root / "pyproject.toml").write_text("", encoding="utf-8")
    monkeypatch.chdir(root)
    result = CliRunner().invoke(cli, ["setup"], input="1\nfred\n")
    assert result.exit_code == 0, result.output
    assert not (root / ".gitignore").exists()


@needs_git
def test_setup_lists_the_env_file_in_the_gitignore_of_its_own_folder(tmp_path, monkeypatch):
    monkeypatch.setattr(keys_check, "check", checker({}))
    root = repository(tmp_path / "project")
    (root / "notebooks").mkdir(parents=True)
    (root / ".env").write_text("", encoding="utf-8")
    monkeypatch.chdir(root / "notebooks")
    result = CliRunner().invoke(cli, ["setup"], input="y\n1\nfred\n")
    assert result.exit_code == 0, result.output
    assert (root / ".gitignore").read_bytes() == b".env\n.env.lock\n"
    assert not (root / "notebooks" / ".gitignore").exists()
    assert "Added .env, .env.lock to" in result.output


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
        ignored = pathlib.Path(".gitignore").exists()
    assert result.exit_code == 0, result.output
    assert "Nothing saved." in result.output
    assert not written
    assert not ignored


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


def test_setup_drops_whitespace_around_a_pasted_key(tmp_path, monkeypatch):
    monkeypatch.setattr(keys_check, "check", checker({}))
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(cli, ["setup"], input="1\n \tfred\t \n")
    assert result.exit_code == 0, result.output
    assert (tmp_path / ".env").read_text(encoding="utf-8") == "FRED_API_KEY=fred\n"


@pytest.mark.parametrize("options", [[], ["--no-check"]])
def test_setup_asks_again_for_a_key_with_a_control_character_inside(tmp_path, monkeypatch, options):
    checked = []

    def check(name, value, **_kwargs):
        checked.append(value)
        return Check(name, Verdict.OK, "fake")

    monkeypatch.setattr(keys_check, "check", check)
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(cli, ["setup", *options], input="1,2\nfr\ted\ny\nfred\nbls\n")
    assert result.exit_code == 0, result.output
    assert "[x]   FRED_API_KEY  it holds a line break, a tab or another control character" in result.output
    assert "fr\ted" not in checked  # a value that cannot be saved is never sent to its source
    assert (tmp_path / ".env").read_text(encoding="utf-8") == "FRED_API_KEY=fred\nBLS_API_KEY=bls\n"


def test_setup_skips_a_key_with_a_control_character_when_asked_to(tmp_path, monkeypatch):
    monkeypatch.setattr(keys_check, "check", checker({}))
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(cli, ["setup"], input="1,2\nfr\ted\nn\nbls\n")
    assert result.exit_code == 0, result.output
    assert (tmp_path / ".env").read_text(encoding="utf-8") == "BLS_API_KEY=bls\n"


def test_setup_says_when_another_program_holds_the_env_file_and_goes_on(tmp_path, monkeypatch):
    monkeypatch.setattr(keys_check, "check", checker({}))
    monkeypatch.setattr(credentials, "LOCK_TIMEOUT", 0.2)
    monkeypatch.chdir(tmp_path)
    with credentials.locked(tmp_path / ".env"):
        result = CliRunner().invoke(cli, ["setup"], input="1,2\nfred\nn\nbls\nn\n")
    assert result.exit_code == 0, result.output
    assert result.output.count("being saved by another program") == 2  # each key is offered, not the first only
    assert "Nothing saved." in result.output
    assert not (tmp_path / ".env").exists()


def test_setup_saves_a_key_on_a_second_try_once_the_env_file_is_free(tmp_path, monkeypatch):
    monkeypatch.setattr(keys_check, "check", checker({}))
    real_save = credentials.save
    calls = []

    def busy_once(env_file, updates):
        calls.append(updates)
        if len(calls) == 1:
            msg = f"{env_file} is being saved by another program; try again"
            raise TimeoutError(msg)
        real_save(env_file, updates)

    monkeypatch.setattr(credentials, "save", busy_once)
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(cli, ["setup"], input="1\nfred\ny\n")
    assert result.exit_code == 0, result.output
    assert len(calls) == 2
    assert (tmp_path / ".env").read_text(encoding="utf-8") == "FRED_API_KEY=fred\n"


@needs_git
@pytest.mark.parametrize("on_disk", [True, False])  # deleted by hand, the file is still in the index
def test_setup_stops_before_saving_when_git_tracks_the_env_file(tmp_path, monkeypatch, on_disk):
    monkeypatch.setattr(keys_check, "check", checker({}))
    root = repository(tmp_path / "project")
    (root / ".env").write_text("FRED_API_KEY=placeholder\n", encoding="utf-8")
    git(root, "add", ".env")
    if not on_disk:
        (root / ".env").unlink()
    (root / "notebooks").mkdir()
    monkeypatch.chdir(root / "notebooks")
    result = CliRunner().invoke(cli, ["setup"], input="y\n1\nfred\n")
    assert result.exit_code == 1, result.output
    assert "tracked by git" in result.output
    assert "git rm --cached .env" in result.output
    assert "Which ones?" not in result.output  # no key is asked for
    assert (root / ".env").exists() is on_disk
    if on_disk:
        assert (root / ".env").read_text(encoding="utf-8") == "FRED_API_KEY=placeholder\n"
    assert not (root / ".gitignore").exists()


@needs_git
def test_setup_saves_once_the_env_file_is_no_longer_tracked(tmp_path, monkeypatch):
    monkeypatch.setattr(keys_check, "check", checker({}))
    root = repository(tmp_path / "project")
    (root / ".env").write_text("FRED_API_KEY=placeholder\n", encoding="utf-8")
    git(root, "add", ".env")
    git(root, "rm", "-q", "--cached", ".env")
    monkeypatch.chdir(root)
    result = CliRunner().invoke(cli, ["setup"], input="1\ny\nfred\n")
    assert result.exit_code == 0, result.output
    assert (root / ".env").read_text(encoding="utf-8") == "FRED_API_KEY=fred\n"


def test_setup_without_git_skips_the_tracking_check(tmp_path, monkeypatch):
    monkeypatch.setattr(keys_check, "check", checker({}))
    monkeypatch.setattr(keys_cli, "GIT", "no-such-git-executable")
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(cli, ["setup"], input="1\nfred\n")
    assert result.exit_code == 0, result.output
    assert (tmp_path / ".env").read_text(encoding="utf-8") == "FRED_API_KEY=fred\n"
