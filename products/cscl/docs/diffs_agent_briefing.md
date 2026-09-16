# Briefing: reducing CSCL dev-vs-prod discrepancies

Context for an agent (or a person) picking up work on closing the gap between our CSCL
build and the legacy ETL's production output. Written so you can start without reading the
whole conversation history that produced it. If anything here conflicts with what you
observe in a live build, trust the live build - this document describes a moving target as
of **2026-09-16**, branch **`ar-more-cscl-diffs-sept-15`**.

## The goal

CSCL (Citywide Street Centerline) is being rebuilt as a dbt/Postgres pipeline
(`products/cscl/`) to replace a legacy ETL. Both pipelines are run against the *same*
source CSCL release, and the legacy ETL's output is the ground truth we're being compared
against ("prod"). The overarching task across many work sessions has been: **find and fix
real bugs that make our output differ from prod**, and for the differences that are real
(not our bugs, not prod's quirks we're choosing not to replicate), **document them clearly**
rather than silently absorbing or hiding them.

This is *not* about making a diff-count go to zero by construction. Several fixes this
project has made were explicitly about making the diff-counting *more honest* (removing
comparison-tool false positives) so that the remaining numbers can be trusted.

## The tooling (read the code, this is just an index)

- **`products/cscl/poc_validation/build_diffs_report.py`** — the top-level report. Merges
  three sources into one `diffs_report.csv` (and a GitHub Actions step summary): dbt
  field-level diffs (`qa__diffs_all` / `qa__diffs_all_summary`), flat-file line diffs
  (`validation_summary.csv`), and GDB row-level diffs (`compare_gdb.py`'s output). Read its
  module docstring first.
- **`products/cscl/poc_validation/compare_gdb.py`** — row-level, per-column comparison for
  every gdb-format export (LION gdb, District gdb). This is where most of the recent fixing
  has happened. Read its module docstring and `_stringify`'s docstring - they explain
  several subtle normalization decisions (float tolerance, timezone-aware datetimes,
  blank-vs-null) that took real investigation to arrive at. Has a `--strict-nulls` CLI flag
  (default is lenient: NULL and whitespace-only strings compare as equal) - use it when you
  want to see whether a null/blank representation choice is hiding something.
- **`products/cscl/seeds/lion_outputs.csv`** — the backbone: one row per output file/gdb
  layer, including a `key_columns` column that declares each gdb layer's real row identity
  (pipe-separated). **Keys are declared here, not auto-detected** - see `seeds.yml`'s doc on
  that column for why, and `compare_gdb.py`'s `_load_declared_keys`/`_guess_key_columns`.
  If you add a new layer or a warning tells you a key is missing, verify one against real
  data and add it here - don't let the tool guess silently.
- **`products/cscl/data_issues.md`** — **the single source of truth for known issues.**
  Every already-understood difference from prod lives here with a stable `CSCL-XXX-NN` ID,
  a status (Open / Accepted / Watch), and a "What would settle it" note. **Read this file
  before investigating anything** - you may be about to re-discover something already
  documented. When you find something new, add an entry here in the same format. When you
  resolve something, update its status rather than deleting the entry (an entry outlives
  the issue that closes it).
- **`products/cscl/docs/prod_bugs/`** — deeper investigation write-ups for specific bugs
  that needed more than a paragraph (e.g. `007-sept-2026-remaining-diffs-investigation.md`).
  `data_issues.md` entries link out to these when relevant.
- **`products/cscl/design_doc.md`** — the actual ETL spec (ported from the legacy system's
  documentation). This is what our pipeline is *supposed* to implement. When behavior looks
  wrong, check here first for whether it's actually specified.

## How to run a build and get results

From `products/cscl/`, with direnv loaded (`source ../../load_direnv.sh` if not already
active in your shell):

```bash
dcp_trigger_build cscl recipe "<a short note>" --no-tail
```

This dispatches the `Build a Dataset: cscl` GitHub Actions workflow on the **current
branch** (`ar-more-cscl-diffs-sept-15` as of writing - confirm with `git branch
--show-current`, don't assume). It prints a run URL; poll it with
`gh run view <run_id> --json status,conclusion`.

**Gotcha:** the workflow's "Check inputs" step runs your `--note` string through an inline
shell script that is *not* properly escaped. Parentheses, quotes, and other shell-special
characters in the note will break the build with a bash syntax error before anything
actually runs (this has happened - see git log around 2026-09-15/16). Keep build notes to
plain words, commas, and hyphens.

Once a build succeeds, its outputs land in S3 (Digital Ocean Spaces, credentials via
`AWS_S3_ENDPOINT`/`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` from direnv). Pull them with
`boto3`:

```python
import boto3, os
s3 = boto3.client("s3", endpoint_url=os.environ["AWS_S3_ENDPOINT"])
bucket = "edm-publishing"
prefix = "db-cscl/build/<branch-name>/validation_output/"
# key files: diffs_report.csv, nyclion_26b_comparison.csv, v26B_Districts_comparison.csv,
# qa__diffs_all_summary.csv, validation_summary.csv
s3.download_file(bucket, prefix + "diffs_report.csv", "/tmp/diffs_report.csv")
```

Dev's raw gdb exports (for direct investigation, not just the comparison CSVs) are at
`s3://edm-publishing/db-cscl/build/<branch>/dataset_files/{nyclion_26b.zip,
v26B_Districts.gdb.zip}`. Prod's are at `s3://edm-private/cscl_etl/<version>/<same
filenames>`. Both buckets use the same S3 client/endpoint.

You can also query the branch's live build schema directly in Postgres - very useful for
tracing a diff back to its SQL source:

```bash
psql "${BUILD_ENGINE_SERVER}/db-cscl" -c "SET search_path TO <branch_schema>; SELECT ..."
```

The schema name is the branch name with dots/dashes turned into underscores, prefixed
appropriately - list them with `psql "${BUILD_ENGINE_SERVER}/db-cscl" -c "\dn"` and look for
one matching your branch. **`BUILD_ENGINE_SERVER` has no dbname in it** - you must append
`/db-cscl` yourself or psql will try to connect to a database named after your username.

## Reading the diffs_report.csv

Columns of interest: `file_id`, `discrepant_rows_from_file_comparison` (file/row-level
count), `accounted_for_discrepant_rows` (explained by a dbt QA model), `unaccounted_discrepant_rows`,
`notes`. A row is worth looking at if `accounted_for != file_level OR unaccounted > 0` - see
`build_diffs_report.py`'s `has_diffs()` for the exact logic (this took two rounds of user
feedback to get right; don't simplify it).

Sort by `gap = abs(file_level - (accounted_for + unaccounted))` descending to find where the
report's bookkeeping itself might be wrong (a large gap usually means something upstream -
often in `compare_gdb.py` - isn't classifying a diff correctly), which is usually more
valuable to fix than chasing raw diff-count size.

## What's already been fixed (don't redo this work)

All landed on `ar-more-cscl-diffs-sept-15`, git log has full detail per commit:

1. **Float-comparison tolerance** in `compare_gdb.py` - `SHAPE_Length`/`SHAPE_Area` are
   recomputed by GDAL on read and differ in trailing digits even for identical geometry.
   Now compared with `rtol=atol=5e-4` instead of exact string equality. This eliminated
   ~150K+ false-positive "modified" rows across every polygon layer.
2. **Timezone-aware datetime normalization** - dev's `CREATED_DATE`-style fields come back
   UTC-aware from `pyogrio`, prod's don't. Fixed `gdb_nyhd` (was 100% "modified") and
   `gdb_nyzip`.
3. **`gdb_nyap.sql` sentinel/leading-zero bugs** - `ASSEMDIST`/`ELECTDIST`/`SCHOOLDIST` were
   inheriting a `nullif`-based sentinel-stripping meant for internal joins elsewhere, and
   `CENSUSBLOCK_2020` was losing leading zeros via an unnecessary int cast. Fixed by
   restoring the stored sentinel values and adding a raw passthrough column (mirroring the
   existing `censusblock_2010_raw` pattern).
4. **Blank-vs-null representation** (the biggest one) - prod's FileGDB export stores "no
   value" as blank/whitespace strings for many text fields; we store real SQL NULL.
   `compare_gdb.py` now treats these as equivalent by default (toggle with
   `--strict-nulls`). This alone cut `gdb_lion`'s flagged-columns count from 75 to 24 and
   `gdb_node`'s "modified" count from 139,674 to 3,824.
5. **`gdb_lion.sql`'s `LegacyID`** - same blank-vs-sentinel pattern as #3/#4, but with a
   numeric sentinel: prod represents "no legacy ID" as the literal string `'0000000'`, we
   produced SQL NULL. Fixed with `coalesce(legacy_segmentid, 0)` before the `lpad`. Left a
   genuine ~2%-of-segments real-value-mismatch residual - see `CSCL-LION-10` in
   `data_issues.md`.
6. **`prod_data_loader.py`** - the generic `load` command now uses the same
   `already_loaded`/`record_load` staleness tracking the other loaders already had, so a
   `production_outputs` table doesn't sit stale indefinitely with no record of what version
   it holds. This is prevention, not a retroactive fix - see the open staleness item below.

## Open targets, roughly in order of likely value

Check `data_issues.md` for the full, current, authoritative list and status - what follows
is commentary on the ones most likely to reward more digging, not a replacement for it.

### 1. `gdb_altnames`/`gdb_lion`'s `Join_ID` diversity gap (`CSCL-LION-09`)

Dev produces only ~16,600 distinct `Join_ID` values where prod has ~32,900 - roughly half.
This is the largest still-unexplained magnitude in the whole report (`gdb_altnames` and
`gdb_node_stname` are the two biggest line items in `diffs_report.csv`). Six things have
been ruled out already (see the `CSCL-LION-09` entry for the full list with numbers) -
**read that before repeating any of the six checks.** The investigation trail currently
ends at: `dcp_cscl_streetname` (raw source) has only 6,927 distinct `facecode` values,
which is the real ceiling on `Join_ID` diversity given the current join logic, and it's
about half what prod's count implies it should be.

Two ways to keep pushing on this:
- **Check `dcp_cscl_featurename`'s facecode contribution too** (the investigation above
  only fully quantified `dcp_cscl_streetname`) - does adding its distinct facecodes close
  more of the gap than expected?
- **Compare against a different CSCL release.** If you have access to a prior version's
  archived `dcp_cscl_streetname`/`dcp_cscl_segment_lgc` (via `dcpy.connectors.edm.recipes`
  or similar), check whether facecode cardinality has historically been this low, or
  whether this specific release's source data is unusually sparse.
- **Read `design_doc.md`'s "Street Code and Face Code" section closely** and check whether
  there's a case in the spec (e.g. non-principal StreetName rows, or a fallback when the
  principal lookup fails) that would produce *more* facecodes than the current
  `principal_flag='Y'`-only join captures.

This one probably needs a GR (Geographic Research team) conversation to fully resolve, but
there may be more code-side evidence to gather first.

### 2. `gdb_lion`'s remaining 24 flagged columns (post blank/null fix)

After fix #4 above, `gdb_lion`'s per-column report (`nyclion_26b_comparison.csv`) shows 24
genuinely-flagged columns, not comparison-tool noise. Most look like a coherent bucket tied
to SAF/roadbed scope (`Street`, `RB_Layer`, `FromLeft`/`ToLeft`/`FromRight`/`ToRight`,
`SAFStreetName`, `SAFStreetCode`, `TrafSrc`, `RBoro`, `L_CD`/`R_CD`, the `LCT1990`/`RCT1990`
family, `MH_RI_Flag`, `ACTIVE_FLAG`, `Carto_Display_Level`) - i.e. genuinely unimplemented
LION spec fields, not bugs. But **`ArcCenterX`/`ArcCenterY` (95.5pp null-rate diff) and
`FeatureTyp` (77.1pp) are *partial* gaps, not total ones** - that shape (some rows have a
value, most don't, when prod has it much more consistently) is what a fixable bug usually
looks like, as opposed to an unimplemented feature. `ArcCenterX`/`Y` in particular is
already tracked as `CSCL-LION-07` ("Center of curvature") but that entry is stale
(`Last verified 26a`) and the doc explicitly says it "was resolved for 25d... at least one
[case] returned in 26a" - worth re-verifying against current 26b data and refreshing the
entry either way.

To regenerate this list yourself: download the current build's LION gdb pair and run
something like

```python
import sys; sys.path.insert(0, "poc_validation")
import geopandas as gpd
from compare_gdb import _effective_isna
# ... load dev/prod 'lion' layer, then for each column:
dev_na, prod_na = _effective_isna(dev[col]), _effective_isna(prod[col])
```

comparing `dev_na.mean()` vs `prod_na.mean()` per column, same as `_compare_layers` does
internally.

### 3. Shoreline-clip fragmentation (`CSCL-DISTRICTS-01`)

`nymcea`, `nypuma2010`, `nypuma2020` all show dev producing roughly 2x as many disjoint
polygon parts as prod after clipping to shoreline, with area matching almost exactly
(<0.002% delta) - meaning it's a fragmentation/minimum-mapping-unit difference, not lost
land. `nynta2020` shows the *same symptom in the opposite direction* (prod is the more
fragmented one there), which breaks the "we always over-fragment" story and hasn't been
explained. The `clipped_geom` macro (`macros/clip_to_shoreline.sql`) already has a
carefully-tuned `min_area=100` sq ft filter that was reverse-engineered to match prod's part
counts on several *other* layers - **do not just lower this further to force a match**; the
existing comment explicitly warns against fitting noise. This one genuinely needs a
decision from whoever owns these layers about minimum mapping unit / dissolve order, per
the doc entry.

### 4. LDF transitory-elimination residual (`CSCL-LDF-01`)

An explicitly GR-owned open item (~3% documented residual, currently measuring closer to
8%). Not a code target - if you find yourself tempted to tweak `int__ldf`-adjacent logic to
close this number, stop and re-read the entry first; the user has already said they'll
follow up with GR directly on this one.

### 5. Stale `production_outputs` tables (SAF/ThinFire)

`production_outputs.saf_*` and `production_outputs.thinfire_*` tables were loaded once via
`prod_data_loader.py`'s generic `load` command *before* fix #6 above added staleness
tracking, so they may not reflect the current CSCL version. `docs/prod_bugs/007-...md`'s
"Update 2026-09-15" note has the detail. **Reloading them is a live, shared-database write
(`production_outputs` is shared across every branch's build)** - do not do this
autonomously. If you're a human (or have been explicitly told to proceed by one), the
command is `python3 poc_validation/prod_data_loader.py load -v <version> -d <table1>
<table2> ...`; re-run the diff report after and update the relevant entries if the
mismatch disappears.

## Conventions and gotchas worth knowing before you start

- **Git**: this branch amends one commit per logical unit of work rather than piling up WIP
  commits (`git commit --amend`, then `git push --force-with-lease`). Follow that pattern
  unless told otherwise, and never force-push without `--force-with-lease`.
- **Don't guess at row identity.** `compare_gdb.py`'s row-level diff needs a real key per
  gdb layer, declared in `seeds/lion_outputs.csv`. A column that happens to be unique in
  today's data is not a real identity - verify against actual data before declaring one.
- **`zsh`'s `$status` is a read-only special variable** - don't use it as a shell variable
  name in scripts/monitors run under zsh; use something like `run_status` instead.
- **pandas 3.0's default string dtype** makes `.astype(str)` preserve `NaN` as a real null
  rather than stringifying it to `"nan"` - this breaks naive `"|".join()` calls and
  before/after comparisons. `_stringify` in `compare_gdb.py` already handles this
  correctly; if you write new comparison code, follow its pattern (per-column
  `.astype(str)` then `.fillna(sentinel)`, not a bulk DataFrame-level fillna).
- **Lint before committing**: `uv run ruff check` / `uv run ruff format` for Python,
  `sqlfluff lint --dialect postgres --templater jinja <file>` for SQL. Some `sqlfluff`
  "Undefined jinja template variable" warnings on files using custom macros (e.g.
  `lion_join_id`) are pre-existing outside a full dbt parse context - check with `git stash`
  whether an error predates your change before treating it as something you introduced.
- **When you find a real fix, verify it against real downloaded data before shipping** -
  don't trust a fix until you've pulled the actual dev/prod gdb pair and run the affected
  comparison function directly in Python. Every fix in the "already fixed" list above was
  verified this way, with before/after row counts, before being committed.
- **When something looks like a data-representation quirk** (null vs blank, null vs a
  sentinel value, float precision, timezone-awareness), check whether it's *systematic*
  (does it affect one column everywhere, or a whole category of columns?) before writing a
  one-off fix. Several of the biggest wins here came from noticing a pattern and fixing it
  once in `compare_gdb.py`'s shared normalization logic rather than patching individual
  layers.
