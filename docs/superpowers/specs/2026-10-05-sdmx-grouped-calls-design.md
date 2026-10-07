# Design — SDMX: several series per call

Date: 2026-10-05
Status: Approved
Builds on: `2026-10-05-store-direct-sources-b1-design.md`, which left "batching several series
per SDMX call" out of scope. This spec brings it in.

## Goal

Make the SDMX source ask for several series in one call, so a sync of the macro catalog's SDMX
series takes about 17 calls instead of 458.

## Why

The acceptance run of group B1 showed two costs of asking one series per call:

- The OECD allows about 60 requests an hour (inferred from its answers: 55 calls went through
  and every later one was refused with HTTP 429). Its 64 series therefore take over an hour at
  one request a minute.
- The BIS takes 211 calls and about seven minutes; each call is also one batch, and the store
  rewrites the source's file after every batch.

Series of one dataflow usually differ in a single position of the key, the country. SDMX lets a
key name several values for a position, joined by `+`. All five providers accept it; this was
checked with live calls using ids of the macro catalog.

| Source | Series | Calls today | Calls grouped |
|---|---|---|---|
| BIS | 211 | 211 | 5 |
| Eurostat | 96 | 96 | 7 |
| IMF WEO | 86 | 86 | 2 |
| OECD | 64 | 64 | 2 |
| ECB | 1 | 1 | 1 |

## Locked decisions

| Decision | Choice |
|---|---|
| Where | Only `store/sources/sdmx.py`. The catalog, the core and the other sources do not change |
| Model | One id is still one series. Grouping is internal to the source |
| Operator | `+` on the one position that varies. Leaving the position open would download every country |
| Safety net | A group that cannot be read as a group is asked series by series, as today |

## Grouping

Requests are grouped in three steps.

1. By dataflow and by the number of parts of the key (parts are separated by `.`).
2. Within such a set, the source picks the position of the key that, when ignored, leaves the
   fewest distinct keys. Requests equal on every other position form a group.
3. A group larger than 50 is cut into groups of at most 50, to keep the address short.

A request whose key already contains `+` or an empty part is never grouped: it goes alone and is
judged as today (a key that returns several series is refused).

A group of one is asked exactly as today.

## One call per group

- The key is the group's common key with the varying position replaced by its values joined by
  `+`, in the order of the requests: `WS_TC/Q.AR+BR+CL.P.A.M.770.A`.
- `startPeriod` is sent only when every request of the group has a `since`; it is the earliest
  year among them.
- The rows of the answer are split by one column: the column, other than `TIME_PERIOD` and
  `OBS_VALUE`, whose values are all among the requested values. If several columns qualify, the
  one with the most distinct values is taken; if two tie, the split is ambiguous.
- Each request then takes the rows whose value in that column is its own, and they are read as
  a single-series answer is read today (frequency from the period text, WEO projections, units,
  a repeated period refused).
- A request with no rows fails with `NOT_FOUND`.
- A group is one `FetchBatch`.

## When a group cannot be read as a group

The group is asked again series by series, exactly as today, in these cases:

- the grouped call answers with an HTTP status other than 200 (one invalid value can make a
  provider refuse the whole call);
- the split is ambiguous or no column qualifies.

An answer with no rows at all is not retried: every request of the group fails with `NOT_FOUND`.

## Errors

- A persistent 429 on a grouped call raises `QuotaExhaustedError`, as on a single call.
- A network failure fails every request of the group with `NETWORK_ERROR`.
- An answer that cannot be parsed fails every request of the group with `SOURCE_ERROR`.

## Pacing

Unchanged: BIS, ECB and Eurostat 30 a minute, IMF 20, OECD 1. With two calls the OECD's pace no
longer costs anything.

## Testing

- Grouping: one varying position; two key patterns in one dataflow; keys of different lengths;
  a cut at 50; keys with `+` or an empty part left alone; a lone series.
- A grouped call: the address, `startPeriod` with and without a `since` on every request, the
  split by column, a request without rows, the WEO projection flag inside a group.
- The safety net: an HTTP error on the grouped call, and an ambiguous split, both followed by
  one call per series.
- Errors: persistent 429, network failure.
- Every existing SDMX test keeps passing unchanged.

## Acceptance criteria

1. Every unit test passes; ruff and mypy are clean; the existing 1,243 tests stay green.
2. The seven live tests of the direct sources pass.
3. A sync of the macro catalog restricted to the six direct sources, into a new store, makes
   about 24 calls (17 SDMX and 7 World Bank) and stores all 762 series.
4. For every one of the 458 SDMX series, the values in that store equal the values downloaded
   series by series on 2026-10-05, apart from observations published in between.

## Out of scope

- Grouping across more than one varying position.
- Grouping DBnomics calls.
- New ids for the 500 series still read through DBnomics (group B2).
