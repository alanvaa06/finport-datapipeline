# Report — DBnomics series moved to their publisher by closeness (2026-10-07)

Follows `2026-10-05-macro-catalog-direct-ids-report.md`, which moved 99 of 500 series under an
equality rule (relative tolerance 1e-4) and left 400 on DBnomics, stale: their datasets (IMF IFS,
IMF DOT, OECD MEI) were retired at the origin, and the publisher's replacement is a re-estimate,
not a copy (the OECD adjusts seasonally again, indexes are rebased, trade is in dollars).

## The rule (`scripts/replace_dbnomics_ids.py --close`)

A candidate moves when, on at least 24 periods in common with the mirror, and reaching a newer
period:

- it is scaled by one, a power of ten (other units) or, for an index (`cpi`, `core_cpi`, `hicp`,
  `ppi`, `ind_prod`, `m1`, `broad_money`), any constant factor (rebased);
- the relative gaps once scaled have a **median within 1 %** and a **90th percentile within 5 %**
  (for rates, a gap below one is measured in points);
- for `cpi`/`core_cpi` the key is a national CPI and for `hicp` an HICP (the IMF's CPI flow has
  both; the closest of the two is not necessarily the one the concept means).

Each moved entry records how and how close in `attrs.close_match`; nothing is renamed and the
alias stays. The industrial production template is new: OECD `DSD_STES@DF_INDSERV`, measure
`PRVM`, activity `BTE`.

The thresholds were set after looking at the distribution: most matches sit below 0.1 % (median)
and 0.5 % (90th percentile); goods trade (DOT against ITG) spreads from 0.1 % to over 10 %, and the
rule keeps the close half.

## Outcome

Output of `report --accept-units --close` (IMF and OECD answers of 2026-10-07, cached):

```
concept                                    no data                no reference publisher has nothing newer             replaced, close   too few periods in common               values differ  total
broad_money                                      0                           0                           1                          23                           0                           1     25
consumer_confidence                              0                           0                           1                          32                           0                           0     33
cpi                                              0                           1                           0                          17                           0                           0     18
exports_goods                                    1                           0                           0                          22                           2                          19     44
fx_usd                                           1                           0                          11                           2                           0                           0     14
hicp                                             0                           0                           0                           8                           0                           1      9
imports_goods                                    1                           0                           0                          19                           2                          22     44
ind_prod                                         7                           2                           0                          17                           0                          10     36
m1                                               0                           0                           0                          19                           0                           6     25
ppi                                              3                           0                          15                           8                           0                           0     26
reserves                                         1                           0                           0                          36                           0                           5     42
short_rate                                       4                           1                          10                           2                           0                           0     17
trade_balance_goods                              1                           0                           0                          32                           0                           9     42
unemployment                                     0                           0                           5                          15                           4                           1     25
total                                           19                           4                          43                         252                           8                          74    400
```

252 moved (IMF 161, OECD 91); 148 stay on DBnomics with `attrs.stale` and the reason.

## Acceptance

The 252 moved entries synced into a scratch store through the store's own SDMX source:

```
[ok]  imf         161 series, 64679 new, 0 revised, 18 calls
[ok]  oecd        91 series, 53042 new, 0 revised, 8 calls
```

Freshness: 230 ok, 22 stale by the monthly threshold (124 days). The IMF publishes the labour
series (`LS`, 20 of them) and some PPIs six to seven months late, and the UK PPI ends in 2023-07;
all of them are still newer than the mirror, which is why they moved.

A store that already held these series under their DBnomics keys keeps that data; the next `sync`
takes the alias from the old key and gives it to the new one.
