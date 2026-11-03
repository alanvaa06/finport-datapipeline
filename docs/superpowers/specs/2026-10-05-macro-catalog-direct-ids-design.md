# Design — Direct ids for the macro series still on DBnomics (group B2)

Date: 2026-10-05
Status: Approved
Builds on: `2026-10-05-store-direct-sources-b1-design.md`, which moved 762 series of the macro
catalog from the DBnomics mirror to their publishers and left 500 behind because their datasets
(IMF IFS, IMF DOT, OECD MEI) were retired at the origin.

## Goal

Point as many as possible of the 500 remaining series at the dataset that replaced theirs at
the publisher, so they stop being stale, without changing what any `e_*` column means.

## Locked decisions

| Decision | Choice |
|---|---|
| When a series counts as replaced | Its values equal DBnomics' on the periods both hold (relative tolerance 1e-6, at least 24 periods in common) and its last period is newer than DBnomics' |
| A series without a replacement | Stays on DBnomics, stale, with `attrs.stale: "no direct source found 2026-10"`; no column disappears |
| What changes in an entry | `source` and `id` only; alias, name, frequency and the other attrs stay |
| Sources considered | The IMF's new SDMX dataflows (CPI, PPI, ER, IRFCL, LS, MFS_IR, IMTS or ITG), the OECD's `DSD_STES@DF_MONAGG` and `DF_CLI`/`DF_BTS`, and Eurostat for European countries where the IMF and the OECD have nothing |

## What is left to replace

| Dataset | Series | Concepts |
|---|---|---|
| IMF IFS | 285 | fx_usd 42, reserves 42, cpi 41, unemployment 39, ppi 37, ind_prod 36, short_rate 27, core_cpi 21 |
| IMF DOT | 132 | exports_goods, imports_goods, trade_balance_goods (44 each) |
| OECD MEI | 83 | consumer_confidence 33, broad_money 25, m1 25 |

## The finder: `scripts/replace_dbnomics_ids.py`

1. **Reference.** Downloads once, from DBnomics' public API (several series a call), the values
   of the 500 series and caches them in a scratch folder.
2. **Candidates.** For each concept, a template found while prototyping, by reading the
   structure of the replacing dataflow: `(source, flow, key pattern)` where the pattern takes
   the country (ISO3 from the entry's `attrs.region`) and the indicator code. One concept may
   have several templates, tried in order.
3. **Verification.** Each candidate is downloaded through the store's own SDMX source, so an
   accepted id already works in the store, and compared with the reference under the rule above.
4. **Writing.** Rewrites `macro.yaml`: accepted entries get their new `source` and `id`; the
   others get the `stale` attr. Prints a report by concept: replaced, kept, and the reason for
   each kept series (no template, no data, values differ, not newer).

The script is idempotent: a series already on a direct source is left alone.

## Testing

- The acceptance rule: equal, different, too few common periods, not newer, no candidate data.
- The country map (region code to ISO3) covers every region of the catalog.
- The YAML rewrite keeps alias, name, frequency and attrs, changes only source and id, and adds
  the stale attr to the rest.
- The catalog test updates its counts; `check_catalog` keeps accepting every entry.

## Acceptance criteria

1. Every unit test passes; ruff and mypy are clean.
2. The finder runs against the real services and its report is kept with the plan.
3. The replaced series sync into a scratch store and `scripts/macro_freshness.py` reports them
   fresh.

Criteria 2 and 3 call public services without keys (several hundred calls; the OECD at one a
minute, grouped).

## Out of scope

- New series or new concepts.
- Replacing a series with a different concept or a different definition.
- Removing series from the catalog.
