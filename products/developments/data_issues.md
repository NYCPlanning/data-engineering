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
| [DEVDB-DET-01](#devdb-det-01) | Determinism | Build output isn't reproducible from the same commit and inputs | Open | 26Q2.1 |
| [DEVDB-DET-02](#devdb-det-02) | Determinism | BIS records tied on the latest `dobrundate` are picked arbitrarily | Open | 26Q2.1 |
| [DEVDB-DET-03](#devdb-det-03) | Determinism | Tied certificates of occupancy are picked arbitrarily | Open | 26Q2.1 |
| [DEVDB-DET-04](#devdb-det-04) | Determinism | `dcpeditfields` lists fields in arbitrary order | Open | 26Q2.1 |
| [DEVDB-MID-01](#devdb-mid-01) | mid_devdb | Duplicate `job_number` rows, picked arbitrarily by `DISTINCT ON` | Open | 26Q2.1 |
| [DEVDB-HNY-01](#devdb-hny-01) | HNY | Two divergent implementations of the same resolution | Open | 26Q2 |
| [DEVDB-HNY-02](#devdb-hny-02) | HNY | Corrections can insert duplicate matches | Open | 26Q2 |
| [DEVDB-HNY-03](#devdb-hny-03) | HNY | Relate flags count rows, not distinct partners | Open | 26Q2 |
| [DEVDB-HNY-04](#devdb-hny-04) | HNY | A `remove` correction can never override an `add` | Open | 26Q2 |
| [DEVDB-HNY-05](#devdb-hny-05) | HNY | 188 corrections rows never apply | Open | 26Q2 |
| [DEVDB-HNY-06](#devdb-hny-06) | HNY | Many-to-one leaves units NULL on all but one job | Open | 26Q2 |
| [DEVDB-HNY-07](#devdb-hny-07) | HNY | Duplicate HPD project_ids double unit counts | Open | 26Q2 |
| [DEVDB-HNY-08](#devdb-hny-08) | HNY | Many-to-many collapse is order-dependent | Open | 26Q2 |
| [DEVDB-HNY-09](#devdb-hny-09) | HNY | Geocodes from different runs drop matches | Open | 26Q2.1 |
| [DEVDB-SRC-01](#devdb-src-01) | Source data | `20260812_internal` HNY archive is Open Data | Open | 26Q2 |

---

## Pipeline map

Orientation for the entries below. HNY runs in three steps inside `02_build_devdb.sh`:

| Step | File | Builds | Consumed by |
|---|---|---|---|
| Union | `sql/_hny_union.sql` | `hpd_units_by_building`, `hpd_geocode_results` — current HNY + historical, with prefixed ids | `_hny_match.sql` |
| Match | `sql/_hny_match.sql` | `hny_geo` (one row per HNY building), `hny_matches` (surviving HNY↔job matches), `hny_no_match`, **`devdb_hny_lookup`** | `final.sql` → the DevDB product columns `HPDAffrdbl`, `HPD_id`, `HPD_jobrelate` |
| Join | `sql/_hny_join.sql` | **`hny_devdb_lookup`** | Exported standalone as `HNY_devdb_lookup.csv`; nothing else reads it |

The two lookup tables are near-anagrams of each other and are easy to confuse. See
[DEVDB-HNY-01](#devdb-hny-01).

Matching runs three ways — BIN+BBL, BBL only, and spatial within 5 m — all requiring HNY
`total_units` within 5 of DevDB `classa_prop`, and excluding demolitions and withdrawn jobs.
Matches are ranked 1–6 by method and job type, the best rank per HNY record and per job
survives, then `hny_corrections.csv` adds and removes pairs by hand.

---

## Determinism

### DEVDB-DET-01

**Build output isn't reproducible from the same commit and inputs** · Open · Last verified
26Q2.1 · Evidence: measured

Two builds from the same commit (`d6bc8d7b`), with the same 32 input versions and the same env
vars, produced different data. Draft `26Q2.1/1` (built 2026-09-13) and build
`dm-devdb-26Q2-determinism` (built 2026-09-14) differ on 2,334 of 807,652 rows in `devdb.csv`
(2,265 DOB NOW, 69 BIS). A third build, `dm-devdb-26Q2` at `dcd549d3`, which only changed
export column names, differs from draft `26Q2.1/1` on 898 rows (830 DOB NOW, 68 BIS). So the
size of the difference varies from build to build.

All 898 rows in the second comparison trace back to places that pick one row among tied
candidates without a tie-breaker. The 2,334-row comparison wasn't broken down.

| Source | Where | Jobs | Entry |
|---|---|---|---|
| Duplicate DOB NOW rows | `final.sql`: `DISTINCT ON (job_number)` with no `ORDER BY` | 829 | [DEVDB-MID-01](#devdb-mid-01) |
| BIS records tied on the latest run date | `_bis.sql`: `row_number()` over `dobrundate` | 67 | [DEVDB-DET-02](#devdb-det-02) |
| COs tied on date and type | `_co.sql`: `latest_co` | 2 | [DEVDB-DET-03](#devdb-det-03) |
| `dcpeditfields` order | `final.sql`: `string_agg` | 7,821, order only, not counted in the 898 | [DEVDB-DET-04](#devdb-det-04) |

Effect on the published HousingDB, draft `26Q2.1/1` vs the `dm-devdb-26Q2` rebuild, ignoring
`DCPEdited` order:

| File | Rows | Rows with changed values | `ClassANet` total |
|---|---|---|---|
| `project_level_external/HousingDB_post2010` | 83,769 → 83,766 | 24 | 569,287 → 569,223 (−64) |
| `project_level_external/HousingDB_post2010_inactive_included` | 111,909 → 111,909 | 33 | 655,654 → 655,627 (−27) |
| `unit_change_summary`, each of the 6 geographies | unchanged | 6 areas | net `permitted` −40, `comp2026` −28, `inactive` +37, `comp2022` +3, `comp2025` +1; largest change in one area 40 |

Most of the changed values are `DateLstUpd` (21 and 29 rows) and `Bldg_Class` (8 and 11 rows).
The 3 jobs missing from the rebuild's `HousingDB_post2010`:

| `Job_Number` | `Job_Status` in draft 1 | `ClassANet` |
|---|---|---|
| 210182443 | 3. Permitted for Construction | 40 |
| 322104334 | 5. Completed Construction | −2 |
| 421804792 | 5. Completed Construction | −1 |

All of the HousingDB changes above come from the BIS and CO ties. The duplicate DOB NOW rows
only vary on columns that aren't in the external HousingDB files (see
[DEVDB-MID-01](#devdb-mid-01)).

Because of this, draft `26Q2.1/2` was made by adding the HPD fields to draft 1's files rather
than by rebuilding. Its `patch_notes.txt` has the details.

**What is still open:** give every row pick a deterministic tie-breaker, decide at each site
which row should actually win, and add a check that fails when two builds from the same inputs
differ.

### DEVDB-DET-02

**BIS records tied on the latest `dobrundate` are picked arbitrarily** · Open · Last verified
26Q2.1 · Evidence: measured

`_bis.sql` keeps one record per job with
`row_number() OVER (PARTITION BY jobnumber ORDER BY dobrundate DESC)` and `gid = 1`. When
several records share the latest run date, their order is undefined. 67 of the 68 BIS jobs that
changed between draft `26Q2.1/1` and the `dm-devdb-26Q2` rebuild have two or more `doc_# = '01'`
records tied on the latest `dobrundate`.

The tied records disagree on status and dates. `104624925` switched from `9. Withdrawn` to
`3. Permitted for Construction`, and `date_lastupdt` on `103008371` moved backward from
2022-05-11 to 2020-01-17.

**What is still open:** which record should win a tie, and whether duplicate records should be
dropped at ingest.

### DEVDB-DET-03

**Tied certificates of occupancy are picked arbitrarily** · Open · Last verified 26Q2.1 ·
Evidence: measured

`latest_co` in `_co.sql` uses
`DISTINCT ON (jobnum) ORDER BY jobnum, effectivedate DESC NULLS LAST, certificatetype`.
Certificates with the same date and type still tie. Two jobs changed `units_co` this way between
draft `26Q2.1/1` and the `dm-devdb-26Q2` rebuild:

| Job | Tied certificates | Draft 1 | Rebuild |
|---|---|---|---|
| `X00676895` | Two final COs on 2024-05-01, with 20 and 18 units | 20 | 18 |
| `103945803` | Two temporary COs on 2025-06-12, with 208 units and NULL | 208 | NULL |

Each of the four most recent temporary CO dates on `103945803` has one row with 208 units and
one with NULL, so the pick can blank out the unit count.

**What is still open:** a tie-breaker, and whether a CO with a NULL unit count should lose to
one with a count.

### DEVDB-DET-04

**`dcpeditfields` lists fields in arbitrary order** · Open · Last verified 26Q2.1 · Evidence:
measured

`final.sql` builds `dcpeditfields` with `string_agg(field, '/')` and no `ORDER BY`. The set of
fields stays the same between builds but the order doesn't: 7,821 rows in `devdb.csv` differ
only in order between draft `26Q2.1/1` and the `dm-devdb-26Q2` rebuild. It doesn't change
meaning, but it buries real differences when comparing two builds.

**What would settle it:** `string_agg(field, '/' ORDER BY field)`.

---

## mid_devdb

### DEVDB-MID-01

**Duplicate `job_number` rows, picked arbitrarily by `DISTINCT ON`** · Open · Last verified
26Q2.1 · Evidence: measured

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
distinct ones. In 26Q2 they agreed on these published columns: `job_type`, `job_status`,
`job_inactive`, `resid_flag`, `classa_init`, `classa_prop`, `classa_net`, `complete_year`,
`permit_year`, `co_latest_units`, `geo_bbl`, `geo_bin`, `latitude`, `longitude` and
`datasource`. **0 of 3,817** jobs had more than one variant.

They don't agree on every published column, though. `final.sql`'s
`DISTINCT ON (mid_devdb.job_number)` has no `ORDER BY`, so which row survives changes between
builds. Between draft `26Q2.1/1` and the `dm-devdb-26Q2` rebuild, 829 of these jobs changed in
`devdb.csv`, all on columns the check above didn't cover:

| Column | Jobs changed |
|---|---|
| `zsfr_prop` | 679 |
| `zsfc_prop` | 537 |
| `zsfcf_prop` | 314 |
| `prkng_init`, `prkng_prop` | 31 each |
| `zsfm_prop` | 20 |

Every changed value is one that job's `mid_devdb` rows carry (1,530 of 1,530). Some duplicates
complement each other rather than conflict: `B00467250` has one row with residential floor area
(4,261) and one with commercial (997), so whichever row wins, the other value is lost.

These columns are in `devdb.csv` and `housing.csv`. `Prkng_Init` and `Prkng_Prop` are also in
the internal HousingDB project-level files. None of them are in the external ones. See
[DEVDB-DET-01](#devdb-det-01) for the other sources of build-to-build variation.

**What is still open:** whether to deduplicate at the DOB NOW source rather than rely on
`DISTINCT ON`, and whether complementary duplicates should be combined (for example floor area
by use) instead of picking one.

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
| Feeds | The DevDB product (`HPDAffrdbl`, `HPD_id`, `HPD_jobrelate`) | A standalone CSV export only |

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


### DEVDB-HNY-09

**HNY and DOB geocodes come from different runs, so lot changes drop matches** · Open ·
Last verified 26Q2.1 · Evidence: measured

`_hny_match.sql` joins the two sides on `h.geo_bbl = d.geo_bbl`, plus `geo_bin` for the
strictest rule. Those columns come from two geocoding runs that happen at different times:

| side | produced by | when |
|---|---|---|
| HNY | `python/geocode_hpd_hny.py` | `developments_datasync.yml`, archived under the HPD extract's version |
| DOB | `python/geocode_dob.py` | inside `02_build_devdb.sh`, during the build |

Both run in `nycplanning/build-geosupport:latest`, but not at the same time, and `latest`
moves. `GEOSUPPORT_VERSION` in `recipe.yml` pins the DCP boundary datasets, not the geocoder.
So when geosupport reassigns a lot between the two runs, the two sides disagree and every BBL
rule stops matching. The job loses its affordable units with no error and no failing test, and
the spatial fallback did not rescue the cases seen so far.

Measured across the two HNY geocode versions behind 26Q2 and 26Q2.1, over the 3,506 addresses
that geocoded successfully in both: 24 had `geo_bbl` reassigned and 68 had `geo_bin`
reassigned, plus 15 and 13 respectively that newly resolved. Most reassignments move a base
lot to a condo billing lot (`3003880021` to `3003880019`, `1007510020` to `1007517502`).

In 26Q2.1 this accounted for 2 jobs and 269 affordable units, on HNY rows that were otherwise
untouched: same address, same borough, same unit counts in both extracts.

A row in `hny_corrections.csv` repairs it, since the `hny_id` is still present in `hny_geo`,
but only once somebody notices the units are missing.

**What is still open:** whether to pin the geocoder image for the length of a release,
geocode both sides in one run, or add a check that flags HNY records whose `geo_bbl` moved
since the previous version.

---

## Source data

### DEVDB-SRC-01

**`20260812_internal` HNY archive is Open Data, not an internal extract** · Open ·
Last verified 26Q2 · Evidence: measured

`datasets/hpd_hny_units_by_building/20260812_internal/` was ingested from Socrata, not from
the `inbox/` extract its version string implies. Its `config.json` records
`source.source.type: socrata` and a single `clean_column_names` step, against the inbox source
and three steps on `20260721_internal`. Both came from `ingest_single.yml`, but the August run
was dispatched from `main`, where the template still points at Open Data
([run 31697178251](https://github.com/NYCPlanning/data-engineering/actions/runs/31697178251)).

The run tagged `latest`, so `get_latest_version` resolves to it. That matters because the
datasync geocode job resolves `latest` rather than taking a version, so
`hny_geocode_results/20260812_internal` is geocoded Open Data too. Builds are unaffected only
because `recipe.yml` pins both datasets explicitly.

**What is still open:** whether to delete or relabel the mislabeled version. While the
template carries a branch-local source, dispatch `ingest_single.yml` from the product branch,
never from `main`.
