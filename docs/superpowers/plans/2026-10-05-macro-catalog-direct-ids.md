# Direct Ids for the Macro Series Still on DBnomics (Group B2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Point the macro catalog's 500 series still on the DBnomics mirror at the dataset that replaced theirs at the publisher, when the values prove to be the same, and say why the rest stay.

**Architecture:** One script, `scripts/replace_dbnomics_ids.py`, with four commands: `reference` caches DBnomics' values; `find` asks the IMF (one wildcard call per country and dataflow, structure-specific XML) and the OECD (one call per dataflow for every country, generic XML) and keeps the closest candidate per series; `report` tabulates; `write` rewrites `macro.yaml` touching only `source`, `id` and `attrs.stale`. The rule (`compare`, `accepted`, `reason`) and the rewrite are pure functions with unit tests; the network part is exercised by running the script.

**Tech Stack:** Python 3.12, httpx, PyYAML, pytest, ruff, mypy strict.

**Spec:** `docs/superpowers/specs/2026-10-05-macro-catalog-direct-ids-design.md`

**What exploration found before this plan was written** (all public, keyless calls):
- The IMF's successors: `CPI` (also `HICP` as index type; `SRP_IX` is the index rebased to a common reference period, which is what IFS published), `PPI`, `ER` (`XDC_USD.EOP_RT`), `IRFCL`, `LS`, `MFS_IR`, `ITG` and `IMTS` (world partner `G001`). Its SDMX endpoint rejects the generic format and `lastNObservations` on wildcards (HTTP 500) but accepts wildcards and `+` with its default structure-specific XML.
- The OECD's: `DSD_STES@DF_MONAGG` (`MANM` M1, `MABM` broad money; `IX` with `ADJUSTMENT=Y` is the seasonally adjusted index) and `DF_CLI` (`CCICP`, consumer confidence). Their series come split in several `Series` elements; the parser merges them.
- Trade: the IMF's `ITG`/`IMTS` values equal DOT's to the fourth decimal but in dollars where DOT was in millions; a unit change is not a replacement under the rule, so those 99 series are annotated `same values in other units`.
- The OECD recomputes seasonal and amplitude adjustments on every publication: differences of 0.1 to 5 per cent, never within tolerance.
- The tolerance was set with the user at 1e-4 (rounding) after seeing what each tolerance buys: 95 series at 1e-6, 97 at 1e-4, 117 at 1e-3, 175 at 1e-2.

**Conventions:** branch `feat/macro-direct-ids`; `.venv/Scripts/python`; mypy as `PYTHONIOENCODING=utf-8 .venv/Scripts/python -m mypy`; commits carry the user's name only.

---

### Task 1: The finder and its tests

**Files:**
- Create: `scripts/replace_dbnomics_ids.py`, `tests/unit/store/replace_dbnomics_ids_test.py`

- [ ] **Step 1: Write the tests** — the period spelling, the rule (accepted, rounding passes 1e-4 and not 1e-6, revisions fail), the reasons (too few periods, nothing newer, other units, no candidate), `best` preferring an accepted candidate then the closest, every catalog region in `ISO3`, every DBnomics concept but `ind_prod` with a template, the rewrite (moves accepted, annotates the rest, leaves the rest byte for byte, idempotent), the two parsers merging split series. The file as committed is the reference.
- [ ] **Step 2: Run them to see them fail**: `.venv/Scripts/python -m pytest tests/unit/store/replace_dbnomics_ids_test.py -q` (the script does not exist).
- [ ] **Step 3: Write the script** as committed.
- [ ] **Step 4: Run them**: `9 passed`; `ruff check .` and mypy clean.

### Task 2: Run the finder against the real services

- [ ] **Step 1**: `.venv/Scripts/python scripts/replace_dbnomics_ids.py reference --cache <scratch>` — 25 calls to DBnomics, 500 series cached.
- [ ] **Step 2**: `.venv/Scripts/python scripts/replace_dbnomics_ids.py find --cache <scratch>` — about 350 calls to the IMF and 3 to the OECD, some 25 minutes.
- [ ] **Step 3**: `.venv/Scripts/python scripts/replace_dbnomics_ids.py report --cache <scratch> --tolerance 1e-4` — expected: 97 replaced; the table is kept in `docs/superpowers/plans/2026-10-05-macro-catalog-direct-ids-report.md`.

### Task 3: Rewrite the catalog

- [ ] **Step 1**: `.venv/Scripts/python scripts/replace_dbnomics_ids.py write --cache <scratch> --tolerance 1e-4` — `97 series moved to their publisher, 403 annotated as stale`.
- [ ] **Step 2**: replace the header comment of `macro.yaml` (it named the deleted converter).
- [ ] **Step 3**: extend `tests/unit/store/macro_catalog_test.py`: `e_ar_fx_usd` now `imf:IMF.STA,ER/ARG.XDC_USD.EOP_RT.M`; 403 entries on DBnomics, every one with `attrs.stale` starting `2026-10: `, no other entry with it.
- [ ] **Step 4**: `ruff check .`, mypy, `pytest -q` all green.

### Task 4: Acceptance

- [ ] **Step 1**: write the 97 moved entries to a scratch catalog and `python -m data_pipeline.store sync --root <scratch>/store --catalog <scratch>/replaced.yaml` (no keys: the IMF needs none). Expected: `[ok] imf ... 97 series`, grouped calls.
- [ ] **Step 2**: `.venv/Scripts/python scripts/macro_freshness.py <scratch>/store` — expected: the 97 series fresh, or stale only where the publisher itself lags.

### Task 5: Documentation and commit

- [ ] CHANGELOG entry; commit script, tests, catalog and docs.
