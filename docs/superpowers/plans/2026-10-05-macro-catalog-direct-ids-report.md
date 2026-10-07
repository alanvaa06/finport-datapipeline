# Report — direct ids for the macro series still on DBnomics (2026-10-05)

Output of `scripts/replace_dbnomics_ids.py report --tolerance 1e-4` after `reference` (25 calls to
DBnomics) and `find` (about 350 calls to the IMF, 3 to the OECD) on 2026-10-05, with
`--accept-units`. 99 of 500 series moved to their publisher (97 equal within rounding, 2 trade
balances equal once the unit change from millions to dollars is allowed, recorded in
`attrs.units_changed`); the other 401 carry `attrs.stale` with the reason below.

```
concept                                   replaced                     no data                no reference                 no template publisher has nothing newer       replaced, other units   too few periods in common               values differ  total
broad_money                                      0                           0                           0                           0                           1                           0                           0                          24     25
consumer_confidence                              0                           0                           0                           0                           1                           0                           0                          32     33
core_cpi                                        11                           1                           0                           0                           0                           0                           0                           9     21
cpi                                             23                           0                           1                           0                           0                           0                           0                          17     41
exports_goods                                    0                           1                           0                           0                           0                           0                           2                          41     44
fx_usd                                          28                           1                           0                           0                          11                           0                           0                           2     42
imports_goods                                    0                           1                           0                           0                           0                           0                           2                          41     44
ind_prod                                         0                           0                           2                          34                           0                           0                           0                           0     36
m1                                               0                           0                           0                           0                           0                           0                           0                          25     25
ppi                                             11                           3                           0                           0                          15                           0                           0                           8     37
reserves                                         0                           1                           0                           0                           0                           0                           0                          41     42
short_rate                                      10                           4                           1                           0                          10                           0                           0                           2     27
trade_balance_goods                              0                           1                           0                           0                           0                           2                           0                          41     44
unemployment                                    14                           0                           0                           0                           5                           0                           4                          16     39
total                                           97                          13                           4                          34                          43                           2                           8                         299    500
```

Reasons, in words:

- **values differ** (299): the publisher's current series is not the one the mirror held. Trade
  from `ITG`/`IMTS` is in dollars where DOT was in millions, and even after that scaling it differs
  by 2 to 50 per cent (DOT mixed partner-reported figures); the OECD recomputes seasonal and
  amplitude adjustments on every publication (0.1 to 5 per cent); some CPI and PPI indexes were
  revised. A first version of this report called 99 trade series "same values in other units": it
  had only checked that the ratio was a power of ten, not the values after scaling. Corrected.
- **publisher has nothing newer** (43): the new id matches but ends where the mirror ends: the
  country stopped reporting (Austria's schilling in 1998, Australia's PPI in 2021, ...).
- **no template** (34): industrial production has no IMF dataflow; Eurostat covers Europe only.
- **no data** (13) and **no reference** (4): the euro area is not a country in the new IMF flows
  (`U2`); four series have no values left at DBnomics either.
- **too few periods in common** (7): the publisher's history starts where the mirror's ends.

Acceptance: the 97 moved series synced into a scratch store in 9 grouped calls to the IMF
(`[ok]  imf         97 series, 59973 new, 0 revised, 9 calls`); freshness by origin:

```
           series  ok  stale  missing  failed  median_age_days
origin                                                        
IMF           320   0      0      320       0              NaN
OECD           83   0      0       83       0              NaN
banxico         6   0      0        6       0              NaN
bis           211   0      0      211       0              NaN
ecb             1   0      0        1       0              NaN
eurostat       96   0      0       96       0              NaN
fred           24   0      0       24       0              NaN
imf           183  73     24       86       0             36.0
inegi           2   0      0        2       0              NaN
oecd           64   0      0       64       0              NaN
worldbank     304   0      0      304       0              NaN
total: 1197 missing, 73 ok, 24 stale
```
