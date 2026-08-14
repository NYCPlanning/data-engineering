# DevDB Data Issues

Known defects in the DevDB build and its source data. One entry per issue, with a stable ID
so code comments, dbt descriptions and issues can point at it and stay valid as this file is
reordered.

Most of what follows is HNY (Housing New York) matching. That code resolves a many-to-many
relationship between HPD affordable-housing buildings and DOB job numbers, and most of the
defects are different ways that relationship gets counted wrong.

## Status vocabulary

| Status | Meaning |
|---|---|
| **Open** | Unexplained, or explained but undecided. Needs work or a decision. |
| **Accepted** | Understood and deliberately not changing. |
| **Watch** | Was resolved, can recur. Check each release. |

**Last verified** is the product version the entry was last checked against — not when it was
written. If it is stale, treat the entry as a hypothesis rather than a finding.

**Evidence** on each entry says how far it was actually confirmed: *measured* (run against
build data), *read* (established by reading the SQL), or *inferred* (deduced from
intermediate output, not yet confirmed directly).

## Index

| ID | Area | Issue | Status | Last verified |
|---|---|---|---|---|
| [DEVDB-MID-01](#devdb-mid-01) | mid_devdb | Duplicate `job_number` rows, masked by `DISTINCT ON` | Open | 26Q2 |
| [DEVDB-HNY-01](#devdb-hny-01) | HNY | Two divergent implementations of the same resolution | Open | 26Q2 |
| [DEVDB-HNY-02](#devdb-hny-02) | HNY | Corrections can insert duplicate matches | Open | 26Q2 |
| [DEVDB-HNY-03](#devdb-hny-03) | HNY | Relate flags count rows, not distinct partners | Open | 26Q2 |
| [DEVDB-HNY-04](#devdb-hny-04) | HNY | A `remove` correction can never override an `add` | Open | 26Q2 |
| [DEVDB-HNY-05](#devdb-hny-05) | HNY | 188 corrections rows never apply | Open | 26Q2 |
| [DEVDB-HNY-06](#devdb-hny-06) | HNY | Many-to-one leaves units NULL on all but one job | Open | 26Q2 |
| [DEVDB-HNY-07](#devdb-hny-07) | HNY | Duplicate HPD project_ids double unit counts | Open | 26Q2 |
| [DEVDB-HNY-08](#devdb-hny-08) | HNY | Many-to-many collapse is order-dependent | Open | 26Q2 |

---

## Pipeline map

Orientation for the entries below. HNY runs in three steps inside `02_build_devdb.sh`:

| Step | File | Builds | Consumed by |
|---|---|---|---|
| Union | `sql/_hny_union.sql` | `hpd_units_by_building`, `hpd_geocode_results` — current HNY + historical, with prefixed ids | `_hny_match.sql` |
| Match | `sql/_hny_match.sql` | `hny_geo` (one row per HNY building), `hny_matches` (surviving HNY↔job matches), `hny_no_match`, **`devdb_hny_lookup`** | `final.sql` → the DevDB product columns `ClassA_HNY`, `HNY_ID`, `HNY_Relate` |
| Join | `sql/_hny_join.sql` | **`hny_devdb_lookup`** | Exported standalone as `HNY_devdb_lookup.csv`; nothing else reads it |

The two lookup tables are near-anagrams of each other and are easy to confuse. See
[DEVDB-HNY-01](#devdb-hny-01).

Matching runs three ways — BIN+BBL, BBL only, and spatial within 5 m — all requiring HNY
`total_units` within 5 of DevDB `classa_prop`, and excluding demolitions and withdrawn jobs.
Matches are ranked 1–6 by method and job type, the best rank per HNY record and per job
survives, then `hny_corrections.csv` adds and removes pairs by hand.

---

## mid_devdb

### DEVDB-MID-01

**Duplicate `job_number` rows, masked by `DISTINCT ON`** · Open · Last verified 26Q2 ·
Evidence: measured

`mid_devdb` holds more than one row for 3,817 job numbers. The duplication enters with the DOB
NOW source and is then multiplied by the joins in `_mid.sql`:

| Table | Rows | Distinct `job_number` |
|---|---|---|
| `_init_bis_devdb` | 267,625 | 267,625 |
| `_init_now_devdb` | 568,488 | 563,949 |
| `_init_devdb` | 836,059 | 831,574 |
| `init_devdb`, `occ_devdb`, `pluto_devdb` | 835,876 | 831,391 |
| `mid_devdb` | 846,428 | 831,391 |
| `devdb_hny_lookup` | 9,728 | 7,772 |
| `final_devdb` | 831,391 | 831,391 |

`_init_bis_devdb` is clean. `_init_now_devdb` carries 4,539 extra rows, and `_mid.sql` then
chains LEFT JOINs on `job_number` across tables that each inherit those duplicates, so counts
multiply rather than add. That is consistent with the per-job row counts in `devdb_hny_lookup`
being perfect squares — 4 (280 jobs), 9 (86), 16 (15), 25 (7), 36 (1): two joined inputs each
carrying *n* rows for a job yield *n²*.

The duplicate rows are **not** identical — the 3,817 jobs carry 18,854 rows but only 8,302
distinct ones. They do, however, agree on everything the product publishes: checked across
`job_type`, `job_status`, `job_inactive`, `resid_flag`, `classa_init`, `classa_prop`,
`classa_net`, `complete_year`, `permit_year`, `co_latest_units`, `geo_bbl`, `geo_bin`,
`latitude`, `longitude` and `datasource`, **0 of 3,817** jobs had more than one variant.

So `final.sql`'s `DISTINCT ON (mid_devdb.job_number)` — which has no `ORDER BY` — currently
picks arbitrarily among rows that agree on every published column, and `final_devdb` lands at
exactly one row per job. The mask works, but it works incidentally: nothing enforces that the
differing columns stay unpublished.

**What is still open:** whether to deduplicate at the DOB NOW source rather than rely on the
mask, and whether the `DISTINCT ON` should get a deterministic `ORDER BY`. Publishing any
column that does differ between duplicates would make output non-deterministic with no warning.

---

## HNY

### DEVDB-HNY-01

**Two divergent implementations of the same resolution** · Open · Last verified 26Q2 ·
Evidence: read, divergence measured

`_hny_match.sql` and `_hny_join.sql` both resolve `hny_matches` into a per-job lookup, using
the same CTE names (`many_developments`, `many_hny`, `relateflags_hny_matches`, `one_to_one`,
`one_to_many`, `many_to_one`) over the same input — and they do it differently:

| | `_hny_match.sql` → `devdb_hny_lookup` | `_hny_join.sql` → `hny_devdb_lookup` |
|---|---|---|
| Case filters | Overlapping: `one_to_many` takes all `one_dev_to_many_hny = 1`, including many-to-many | Disjoint: each of the four flag combinations handled separately |
| Many-to-many | Folded into the other two branches | Explicit `_many_to_many` → `many_to_many` two-step |
| `hny_id` for grouped rows | Literal `'Multiple'` | `string_agg` of the real ids |
| Columns | 5 | 24 — adds income bands, bedroom mix, project dates |
| Feeds | The DevDB product (`ClassA_HNY`, `HNY_ID`, `HNY_Relate`) | A standalone CSV export only |

The overlapping filters in `_hny_match.sql` are what made the 26Q2 build fail: its
`one_to_many` grouped on a per-row flag that is not constant within its own filter. The
disjoint version in `_hny_join.sql` has the same `GROUP BY` shape but cannot hit the bug,
because its `WHERE` pins both flags.

They do disagree. Compared on `job_number` in the 26Q2 build:

| | Jobs |
|---|---|
| Agree on `classa_hnyaff` | 7,742 |
| **Disagree on `classa_hnyaff`** | **6** |
| Present only in `devdb_hny_lookup` (ships) | 24 |
| Present only in `hny_devdb_lookup` (CSV) | 1 |

`hny_devdb_lookup` is one row per job (7,749 / 7,749). `devdb_hny_lookup` is 9,728 rows for
7,772 jobs, inheriting the duplication in [DEVDB-MID-01](#devdb-mid-01).

So the richer, cleaner implementation is the one that does *not* reach the product, and the
two disagree about 6 jobs' affordable unit counts.

**What is still open:** which of the two is right for those 6 jobs, and whether
`_hny_match.sql` should delegate to one shared implementation rather than keep a second copy.

### DEVDB-HNY-02

**Corrections can insert duplicate matches** · Open · Last verified 26Q2 · Evidence: measured

`hny_corrections.csv` is applied in two statements. The `DELETE` drops a pair if any of its
rows says `remove`. The `INSERT` that follows selects `FROM hny_corrections` filtered only on
whether the *pair* appears in the add list — it never filters the rows it iterates by action.
A pair listed twice therefore inserts twice.

Measured on 26Q2: job `320909852` was listed both `add` and `remove` for HNY `58555/927153`,
which put two identical rows in `hny_matches`. Injecting the same shape onto job `121204464`,
which had a single clean match, took `classa_hnyaff` from 297 to 594 and relabelled it
many-to-many — with no error raised.

Whether a conflicting pair actually doubles anything depends on it reaching the `INSERT`: a
pair already produced by automated matching, or whose `hny_id` is absent from `hny_geo`,
inserts nothing. Four such pairs sit in the file today and are inert for those reasons, which
is why `assert_no_conflicting_hny_corrections` warns rather than blocks.

The corrections CSV is an export of a workbook the Housing team maintains, so fixes applied
to the CSV are lost on the next export.

**What would settle it:** filter the `INSERT` by `btrim(action) = 'add'`. That makes a
duplicate row a no-op instead of a doubling, independently of whatever the workbook contains.

### DEVDB-HNY-03

**Relate flags count rows, not distinct partners** · Open · Last verified 26Q2 · Evidence: read

`many_developments` and `many_hny` use `HAVING count(*) > 1` on `hny_matches`. Duplicate rows
for a single pair therefore look identical to a genuine many-to-many relationship: one HNY
record matched twice to the *same* job is flagged as shared *across* jobs.

This is what turns [DEVDB-HNY-02](#devdb-hny-02) from a harmless duplicate into a wrong relate
label, and it is why the 26Q2 failure presented as a mixed-flag job rather than as an obvious
duplicate.

**What would settle it:** `count(DISTINCT job_number)` and `count(DISTINCT hny_id)`
respectively. Both files carry the same pattern.

### DEVDB-HNY-04

**A `remove` correction can never override an `add`** · Open · Last verified 26Q2 ·
Evidence: read

The `DELETE` runs before the `INSERT`. A pair listed both ways is deleted and then immediately
reinstated, so `remove` has no effect whenever an `add` exists for the same pair. A `remove`
row only does anything when automated matching produced the pair on its own.

This makes the corrections file unable to express "never match these two", which is the
operation someone reaches for when they see a bad automated match that a previous correction
also added.

**What would settle it:** decide the precedence rule — most likely `remove` wins — and apply
it in one pass rather than two statements.

### DEVDB-HNY-05

**188 corrections rows never apply** · Open · Last verified 26Q2 · Evidence: measured

`hny_corrections.action` holds `'add '` with a trailing space for 188 of 1,452 rows. Both the
`DELETE` and the `INSERT` compare `action = 'add'` / `= 'remove'` exactly, so those 188
corrections are silently ignored. Confirmed by grouping `action` in the build database: 1,207
`add`, 188 `add `, 58 `remove`.

Fixing this will *add* matches, so it changes published unit counts and should not be done
alongside an unrelated release.

**What would settle it:** `btrim(action)` in both statements, then diff `classa_hnyaff` across
a before/after build to size the change.

### DEVDB-HNY-06

**Many-to-one leaves units NULL on all but one job** · Open · Last verified 26Q2 ·
Evidence: read

When one HNY record matches several jobs, `_hny_match.sql` assigns the units to
`min(job_number)` and gives every other matched job a NULL `classa_hnyaff` — the `CASE` has no
`ELSE`. That is deliberate anti-double-counting, but the choice of `min(job_number)` is
arbitrary rather than meaningful, and a job in a many-to-many cluster can end up NULL while
its own exclusively-matched HNY units go uncounted anywhere.

`_hny_join.sql` handles the same case differently again: it emits one row per `hny_id` via
`min(job_number)`, dropping the other jobs from the output rather than NULLing them.

**What would settle it:** agree what the correct attribution is when several jobs legitimately
share one HNY building, then make both files implement it — or collapse them per
[DEVDB-HNY-01](#devdb-hny-01).

### DEVDB-HNY-07

**Duplicate HPD project_ids double unit counts** · Open · Last verified 26Q2 · Evidence: read

HPD sometimes publishes one physical project under two `project_id`s — e.g. 44223 "ROCHESTER
SUYDAM PHASE 1" and 70913 "ROCHESTER SUYDAM PHASE I". When both copies survive matching, the
job is flagged one-dev-to-many-hny and the unit fields are summed, so `classa_hnyaff` comes
out at twice the real count.

Neither existing safeguard catches it: the match-priority filter only separates the copies
when they score differently, and the corrections guard only checks whether that exact
(`hny_id`, `job_number`) pair is already present, not whether the job already has a match for
the same building under another `project_id`.

Detected by `assert_no_duplicate_hny_projects_matched`, which warns.

**What would settle it:** a canonical-project decision from HPD, or a rule for choosing
between two copies that geocode equally well.

### DEVDB-HNY-08

**Many-to-many collapse is order-dependent** · Open · Last verified 26Q2 · Evidence: read

`_hny_join.sql` resolves many-to-many in two steps: group by `job_number` to build a
`string_agg` array of `hny_id`s, then group by that array to collapse jobs. The existing
comment in the file notes the caveat directly — the sequence of ids in the array affects the
result and does not guarantee a unique record.

The `ORDER BY r.hny_id ASC` inside the `string_agg` makes the array itself deterministic, so
the residual risk is jobs whose HNY sets are *not* identical but overlap, which the array
equality then treats as unrelated.

**What would settle it:** decide whether overlapping-but-unequal HNY sets should collapse
together, and replace array equality with an explicit cluster identity if so.
