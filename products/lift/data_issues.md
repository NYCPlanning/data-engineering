# LIFT Data Issues

Known problems in LIFT's source data, with a stable ID per issue so code comments and dbt descriptions can point at one.
This file records what we see and what would settle it.
It isn't a work tracker.

## Status vocabulary

| Status | Meaning |
|---|---|
| **Open** | Unexplained, or explained but undecided. Needs work or a decision. |
| **Accepted** | Understood and deliberately not changing. |
| **Watch** | Was resolved, can recur. Check each release. |

**Last verified** is the product version the entry was last checked against.

## Index

| ID | Column | Issue | Status | Last verified |
|---|---|---|---|---|
| [LIFT-01](#lift-01) | `moa_date` | 2-digit years and Excel `####` values | Open | 26v1 |
| [LIFT-02](#lift-02) | `redev_priority` | Mixes time horizon, target quarter, and strategy | Open | 26v1 |
| [LIFT-03](#lift-03) | `juris` | Blank on lots the City leases, though the dictionary says blank means city owned | Open | 26v1 |

## LIFT-01

`moa_date` (date the lot was vested in city ownership) can't be parsed into a date.

- The data dictionary says `MM/DD/YYYY`, but every populated value is `M/D/YY`.
  Years cover all 100 two-digit values with no gap, so the century can't be inferred: `23` could be 1923 or 2023.
- 316 rows hold a run of `#` characters, which is how Excel displays a value too wide for its column.
  All 316 still have a `moa_method`, so a date existed upstream.

Both are already in the raw `dcas_lift.csv` (`edm-private`, version `20260804`), which was saved from Excel (it starts with a UTF-8 BOM).
`dcas_ipis` in `edm-recipes` doesn't carry an MOA date, so there's no second source to recover it from.

**What would settle it:** DCAS re-exporting the CSV with the date column formatted as 4-digit years and wide enough to avoid `####`.

## LIFT-02

`redev_priority` (Public Sites `Prioritization`) holds three kinds of value in one column:

| Kind | Examples | Rows |
|---|---|---|
| Time horizon | `Near-Term`, `Medium-Term`, `Long-Term`, `Long-term`, `Long-Term - Outlier` | 85 |
| Target quarter | `Q3 2026` through `Q4 2029` | 32 |
| Strategy or status | `TDRs`, `Active`, `Not Viable`, `Announced this Admin`, ... | 180 |

That makes it unusable as a single category filter or widget.
The planned fix is to split it into separate columns in `lift_supplemented` while keeping the original, batched with other higher-impact build changes.
The source field mapping is also unconfirmed (see README Limitations).

## LIFT-03

The data dictionary says a blank `juris` means the lot is "city owned but under the jurisdiction of a non-city entity like a state authority".
But 22 of the 23 lots whose IPIS agency use records are all leased (`ownedleased` all `L`) have a blank `juris`, e.g. the World Trade Center and Stuyvesant High School.
So a blank `juris` doesn't reliably mean the City owns the lot.

`lift_supplemented_map.owned_leased` reads ownership from `ownedleased` instead.

**What would settle it:** DCAS confirming what a blank `juris` means for lots the City only leases.
