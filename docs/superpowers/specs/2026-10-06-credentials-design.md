# Design — Credentials: one `.env` per project, `setup`, `keys`, and checked keys

Date: 2026-10-06
Status: Approved

## Goal

A person who installs the library, or an agent working in a project folder, gets every key the
library uses from one `.env` in the project folder, can fill it with one guided command, and
learns at that moment whether each key works, instead of on the first failed download.

## Today (what this replaces)

Three readers of credentials disagree:

| Reader | Reads from | Knows |
|---|---|---|
| Equity entry script (`templates/data_pipeline/__main__.py`) | `Config/.env` copied into `os.environ` by `load_config_env`, then `os.getenv` | FMP, LSEG |
| Store (`store/keys.py`) | process environment, then `./.env` | FRED, BLS, BANXICO, INEGI, COMTRADE, SEC_EDGAR_UA |
| Panel (`equity/services/config_editor.py`) | reads and writes `Config/.env` | FMP, LSEG, FRED, BANXICO, INEGI |

The panel cannot save `BLS_API_KEY`, `COMTRADE_API_KEY` or `SEC_EDGAR_UA`, which the store needs.

## Locked decisions

| Decision | Choice |
|---|---|
| Where keys live | One `.env` at the root of the project folder. No user-wide or machine-wide file: each project governs its own keys |
| Which value wins | 1. a value passed in code, 2. the process environment, 3. the nearest `.env` |
| Nearest `.env` | The current folder's, else its parent's, and so on up to the filesystem root; the first one found is used (a notebook in `project/notebooks/` finds `project/.env`) |
| Where `setup` and the panel write | The nearest `.env`; when there is none, a new `.env` in the current folder |
| `Config/.env` | Gone. `init` writes `.env` at the root |
| Checking keys | `setup` and the panel check every key they save, with one request to its source |
| Compatibility | None needed: the library has no users yet |

## Units

| Unit | Purpose | Depends on |
|---|---|---|
| `data_pipeline/credentials.py` (new) | The registry of known keys; finding the nearest `.env`; resolving values with their origin; saving values into a `.env` without losing its other lines | `python-dotenv` only. Imports neither engine |
| `data_pipeline/keys_check.py` (new) | Checking one key against its source | `credentials`, the store's sources and HTTP client. Lives next to `cli.py`, the only other module that knows both engines |
| `data_pipeline/keys_cli.py` (new) | The `setup` and `keys` commands, added to the one console script | `credentials`, `keys_check` |
| `store/keys.py` | Removed; its users import `credentials` | |
| `equity/modules/dotenv_loader.py` and `load_config_env` | Removed | |

### Registry

One frozen record per key: `name`, `label` (the source, as people know it), `scope` (what it
unlocks), `how` (how to get it, as the start of a sentence), `secret` (False only for
`SEC_EDGAR_UA`, a User-Agent sent in clear by design).

| Name | Unlocks | Get it at |
|---|---|---|
| `FMP_API_KEY` | Financial Modeling Prep: prices and fundamentals | https://site.financialmodelingprep.com/developer/docs/stable |
| `LSEG_APP_KEY` | LSEG Workspace: prices and fundamentals | The App Key Generator inside LSEG Workspace |
| `FRED_API_KEY` | FRED: United States macro | https://fredaccount.stlouisfed.org/apikey |
| `BLS_API_KEY` | BLS: United States labour and prices | https://data.bls.gov/registrationEngine/ |
| `BANXICO_TOKEN` | Banxico SIE: Mexico macro | https://www.banxico.org.mx/SieAPIRest/service/v1/token |
| `INEGI_TOKEN` | INEGI: Mexico macro | https://www.inegi.org.mx/app/api/indicadores/interna_v1_1/tokenVerify.aspx |
| `COMTRADE_API_KEY` | UN Comtrade: goods trade | https://comtradedeveloper.un.org/ |
| `SEC_EDGAR_UA` | SEC filings and XBRL facts (no key: a User-Agent with a contact) | https://www.sec.gov/os/accessing-edgar-data |

Every list of keys in the code (the panel's, the store's, the entry script's) reads this registry.

### Resolution

```python
def find_env_file(start: pathlib.Path | None = None) -> pathlib.Path | None: ...
def resolve(
    explicit: Mapping[str, str] | None = None,
    environ: Mapping[str, str] | None = None,   # defaults to os.environ
    env_file: pathlib.Path | None = None,       # defaults to find_env_file(start)
    start: pathlib.Path | None = None,          # where the search for the .env starts; defaults to the current folder
) -> Credentials: ...
```

`Credentials` keeps, per key, the value and its origin (`code`, `environment`, or the `.env`
path). Its `repr` lists names and whether each is present, never values; `secrets()` returns the
values to scrub from messages, as `store/keys.py` does today. Empty values count as absent.

`save(env_file, updates)` replaces the line of each updated name, appends names that were not
there, keeps comments and unknown lines, refuses names outside the registry and values with line
breaks. It is the panel's `save_env_values`, moved.

### Consumers

- `Store(root, catalog, *, credentials=None, env_file=None, ...)`: `credentials` is a mapping of
  explicit values; `env_file` picks a file instead of the nearest one. `sync()` calls `resolve`.
- `MacroStore` passes the same two arguments through.
- The entry script template calls `credentials.resolve()` once and hands
  `creds.get("FMP_API_KEY")` and `creds.get("LSEG_APP_KEY")` to the providers.
- The panel reads and writes the nearest `.env` through `credentials`, and shows all 8 keys.

## Checking a key

```python
class Verdict(enum.StrEnum):
    OK = "ok"              # the source answered with data
    REJECTED = "rejected"  # the source said the key is wrong
    UNKNOWN = "unknown"    # could not tell: no network, a 5xx, LSEG Workspace not running

def check(name: str, value: str, *, transport: httpx.BaseTransport | None = None, today: datetime.date | None = None) -> Check: ...
```

`Check` carries the verdict and a one-line ASCII detail, scrubbed of the value.

- Store sources: build the source with a throwaway `Client` and credentials holding only this
  value, call `fetch` with one probe request, and read the outcome: data -> `OK`,
  `Outcome.KEY_ERROR` -> `REJECTED`, anything else -> `UNKNOWN`. No storage is opened, so no store
  file and no persisted request budget changes. The rejection rules already in each source are
  reused; no new per-source HTTP code.
- Probes: FRED `UNRATE`; BLS `CUUR0000SA0`; Banxico `SF43718`;
  Comtrade Mexico, annual, two years before the current one, world partner, total; SEC a GET of
  the public ticker list (`company_tickers.json`) with the User-Agent, through `sec.Edgar` (a value
  with no `@` is `REJECTED` without a request: the SEC asks for a contact e-mail).
- FMP: one request of its own to `stable/search-symbol` (the equity provider's search endpoint) for
  `AAPL`, limit 1. A 401 or 403, or a 200 carrying an `Error Message`, is `REJECTED`; a list is `OK`;
  any other answer is `UNKNOWN`. It does not use the provider's `validate_api_key()`.
- INEGI is never checked: its BISE endpoint answers with real data for any token (verified live
  2026-10-06), so a probe would report `OK` for a wrong token. The verdict is always `UNKNOWN`.
- LSEG is not requested: its key is checked when Workspace opens a session, so the verdict is
  always `UNKNOWN`.
- A check never raises: every exception becomes `UNKNOWN` with its scrubbed message.

## `data-pipeline setup`

```
$ data-pipeline setup
Keys are saved in C:\mi_proyecto\.env

  1  FRED_API_KEY       FRED: United States macro          missing
  2  BANXICO_TOKEN      Banxico SIE: Mexico macro          set
  ...
Which ones? (numbers separated by commas, Enter for all missing): 1
FRED_API_KEY (Get one at https://fredaccount.stlouisfed.org/apikey) (Enter to skip): ********
[ok]  FRED_API_KEY  works
Saved in C:\mi_proyecto\.env.
```

- `setup` is interactive. Secret values are typed hidden, and on Windows the hidden prompt reads
  the console, so a secret key cannot be fed through a pipe. For `SEC_EDGAR_UA` it asks for a name
  and an e-mail and saves `Name email`.
- Each key is saved as soon as it is accepted, so an interruption keeps what was already accepted.
  Enter at a key's prompt skips that key.
- A key that is already set is listed as "set"; choosing it asks before replacing. When the key is
  set in the environment, which wins over the file, it asks whether to save one in `.env` anyway.
- `[x]` asks to type it again or skip; `[?]` saves it and says it could not be checked.
- `--no-check` saves without checking (the SEC's local rule, a contact e-mail, still applies).
  Output is ASCII only.

## `data-pipeline keys`

A first line `.env: <path>` (or `.env: none found here or in a parent folder (run: data-pipeline setup)`), then
one line per registry key: `[ok]` or `[ ]`, the name, its label, and the origin (`environment` or the `.env` path; a
value passed in code cannot exist for a command). Never values. Exit code 0.

## Panel

- `GET /api/env` returns the 8 keys as a list of `{name, set, label, scope, secret}` (labels come
  from the registry, so the page drops its own copies). It reports only what is in the `.env` file:
  a key set only in the environment shows as not set (`data-pipeline keys` shows the environment).
- `POST /api/env` refuses values that are not text, saves, then checks each saved key in parallel
  and returns one verdict and detail per key.
- The page shows each saved field as Works, Rejected (with the reason next to the key) or Saved,
  not checked, with the detail. Saving is not blocked by a rejection or an unknown verdict.
- The panel uses the nearest `.env` from the folder it was started in.

## Workspace

- `init` and `start` write `.env` (from the template, with the 8 names, empty) and add `.env` to the
  `.gitignore` at the root (append-only), unless they are there. `start` prints a hint while a
  `Config/.env` still exists, which is no longer read. `update` treats them like the other workspace
  files.
- `templates/data_pipeline/Config/.env` moves to `templates/data_pipeline/.env`.

## Errors

A missing key keeps today's rule (only the source that needs it stops) and its message names the
key, where to get it and the command:

```
FRED_API_KEY is missing. Get one at https://fredaccount.stlouisfed.org/apikey and run: data-pipeline setup
```

The same text replaces "missing in .env" in the store. The equity resolver's message, which has no
registry key at hand, names only the command: "Data provider X requires an API key. Run:
data-pipeline setup (or set it in the nearest .env, in this folder or a parent)." It replaces the
"Set it in the Config/.env file" message.

## Breaking changes

- `data_pipeline.equity.load_config_env` and `Config/.env` are removed.
- `data_pipeline.store.keys` is removed; `data_pipeline.credentials` replaces it.

## Testing

All offline.

- `resolve`: each layer wins over the next; empty values are absent; origins are reported.
- `find_env_file`: found in the current folder, in a parent, and not found.
- `save`: replaces, appends, keeps comments, refuses unknown names and line breaks.
- `check`: the outcome-to-verdict rule with fake sources (data, key error, network error, quota,
  exception); FRED end to end with `httpx.MockTransport` (accepted, rejected, unreachable); SEC
  (accepted, refused, no e-mail); FMP through `httpx.MockTransport` (401, 200 with an `Error
  Message`, a list, unexpected); INEGI and LSEG (always unknown, no request). Each store
  source's own rejection rule is already covered by its tests.
- `setup` and `keys` through Click's `CliRunner` with scripted input, checks stubbed.
- Panel: `/api/env` returns verdicts; the existing panel tests move from `Config/.env` to `.env`.
- Store and equity: the existing tests keep passing with `credentials` in place of `keys`.

## Out of scope

- A user-wide or machine-wide credentials file, and the OS keyring.
- Searching the catalog, a default shared store, JSON output in the CLI, an agent guide or MCP
  server, and exporting current values to SQL: the next spec.
