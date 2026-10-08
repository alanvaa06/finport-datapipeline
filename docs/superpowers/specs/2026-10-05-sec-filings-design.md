# Design — SEC filings and the document kind

Date: 2026-10-05
Status: Approved
Builds on: `2026-10-01-finport-datapipeline-design.md`, which designed three kinds of data, and
`2026-10-05-sec-xbrl-design.md`, whose SEC plumbing this source shares. This is the third and
last piece of "Comtrade and SEC", and the first source of the document kind.

## Goal

Keep in `data_pipeline.store` the filings a company sends to the SEC, as the files the SEC
publishes, downloaded once and never changed, with a list that says what each file is.

## Locked decisions

| Decision | Choice |
|---|---|
| What is stored | The original file, byte for byte: the primary document of each filing and, for an 8-K, its exhibits (the earnings release is an exhibit). No conversion to text, no sections |
| Default scope | Forms 10-K, 10-Q, 8-K, 20-F and 40-F, amendments included, filed in the last 10 years |
| Wider scope | Catalog fields `forms`, `amendments` and `start` |
| One catalog id | One company, written as its ticker in upper case, as for `sec_xbrl` |
| Versions | None. A filing never changes at the SEC: a file is downloaded once and never rewritten |
| A filing that fails | Is recorded as the company's failure and skipped; the later filings still download, and the next run asks for it again |

## Catalog

```yaml
- source: sec_filings
  ids: [AAPL, MSFT]
```

| Field | Default | Meaning |
|---|---|---|
| `forms` | `[10-K, 10-Q, 8-K, 20-F, 40-F]` | Forms to download |
| `amendments` | `true` | Also the amendments of those forms (`10-K/A`) |
| `start` | today minus 10 years | First filing day to download. This is the catalog's shared `start` field |

The key of an entry is `sec_filings:<TICKER>`. The credential is `SEC_EDGAR_UA`, as for
`sec_xbrl`.

## On disk

```
documents/sec_filings/AAPL/0000320193-23-000106/aapl-20230930.htm
documents/sec_filings/AAPL.parquet
```

A document is a group of files: here, one filing, named by its accession number. Files keep
the name the SEC gives them. The list (`<id>.parquet`) has one row per file:

| Column | Meaning |
|---|---|
| `group` | The accession number of the filing |
| `date` | The day the SEC received the filing |
| `file` | Name of the file |
| `role` | `primary` or `exhibit` |
| `form` | Form of the filing (`10-K`, `8-K/A`, ...) |
| `period` | The filing's report date, when the SEC gives one |
| `url` | Where the file was downloaded from |
| `size` | Bytes |
| `sha256` | Hash of the content |
| `fetched_at` | When it was downloaded |

A file is written under a temporary name and moved into place. A file that exists is never
written again. A filing is in the list only when all of its files are on disk.

## What a sync does

For each company the source receives the groups already in the list and:

1. asks for the company's list of filings (`GET https://data.sec.gov/submissions/CIK<10>.json`,
   and its older pages only when they reach back to `start`);
2. keeps the filings of the wanted forms filed on or after `start` that are not stored, oldest
   first;
3. for each, downloads the primary document from
   `https://www.sec.gov/Archives/edgar/data/<cik>/<accession without dashes>/<file>`; when the
   SEC names no primary document (some filings before 2001), the filing's complete text
   (`<accession>.txt`);
4. for an 8-K, lists the filing's folder (`index.json`) and downloads every `.htm`/`.html` that
   is not the primary document, an index page or a page of the XBRL viewer (`R<n>.htm`).

One filing is one batch, stored as soon as it arrives, so a run that stops resumes by itself. A
company with nothing new costs one call.

A filing that fails (a file the SEC lists but does not have, an answer that cannot be read, a
file name the store refuses, a network failure) fails the company for this run with the
filing's accession number in the reason, and the source goes on with the next filing: one
filing that always fails never holds back the ones filed after it. The third network failure in
one company's run ends that company's run, since the SEC is then likely unreachable. `sync(full=True)` changes nothing for documents: a
stored filing is never asked for again.

`requests_per_minute = 300`, no daily budget. HTTP 403 and a persistent 429 stop the source, as
for `sec_xbrl`. An unknown ticker fails that entry with `NOT_FOUND`.

The index row of a company: kind `document`, name from the SEC, `attrs.cik`, the newest filing
day as last period, stale after 140 days without a filing unless the entry sets its own
threshold.

## Reading

```python
store.documents("sec_filings", "AAPL", form="10-K")      # the list, with the path of each file
store.documents("sec_filings", "AAPL", as_of="2024-03-01")  # what had been filed by that day
store.documents("sec_filings")                             # every company
```

Returns the rows of the list with two more columns, `id` (the company) and `path` (the file on
disk), oldest filing first. Filters keep the rows whose column equals the value. `show
sec_filings:AAPL` prints the citation and the ten newest files.

## Changes to the core

- `model`: `DocumentFile` (name, content, url, role), `Document` (group, date, files,
  attributes), `DocumentData` (entry, key, name, documents, staleness threshold, attrs),
  `FetchBatch.documents`, `Request.groups` (the groups already stored). File and group names are
  checked when built: letters, digits, `.`, `_` and `-` only, never `.` or `..`.
- `storage`: writing a file once, atomically; reading and writing the list of an id.
- `sync`: for a source of kind document, requests carry `groups`; each document of a batch is
  written and its rows added to the list. Counts, lock and report work as for tables; `new`
  counts files.
- `api` and `cli`: `Store.documents`, and `status` and `show` for documents.
- `sources/sec.py`: what `sec_xbrl` and `sec_filings` share (tickers to CIK, the User-Agent,
  403 and 404). `sec_xbrl` keeps its behavior and its tests.

## Testing

- Model and storage: unsafe names refused; a file written once and not rewritten; the list
  round trip.
- Source: defaults and every field with its validation; forms and amendments; `start`; filings
  already stored skipped; older pages asked only when needed; the 8-K exhibit filter; a filing
  without a primary document; missing credential, unknown ticker, 403, 404, a failed download.
- Sync, API and CLI: files on disk byte for byte, the list, the index row, a second sync that
  downloads nothing, a stop in the middle that resumes, `documents` with filters and as-of,
  `status`, `show`, `series` and `table` refusing a document key.
- One live test, off by default.

## Acceptance criteria

1. Every unit test passes; ruff and mypy are clean; the existing 1,373 tests stay green.
2. The live test passes with the user's User-Agent.
3. A load of AAPL and SAP with `start` three years back completes, and a second sync right
   after downloads nothing and makes one call per company (plus the list of tickers).
4. For AAPL in that range, the store holds the same documents as
   `research_analyst/tools/sec_fetch.py` (run with amendments), with identical bytes.

Criteria 2 to 4 call the SEC (about 250 calls) with the User-Agent the user approved.

## Out of scope

- Converting documents to text, splitting sections, search.
- Exhibits of 10-K and 10-Q; every file of a filing.
- Form 6-K by default (it can be asked for with `forms`).
- Earnings-call transcripts (they are not on EDGAR).
- A limit on the size of a file.
