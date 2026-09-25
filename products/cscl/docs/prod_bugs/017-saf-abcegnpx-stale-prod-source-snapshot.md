# Bug 017: `saf_abcegnpx_*`'s Row-Count Diffs - Traced, Not GR-Confirmed

**Status:** Open - two distinct mechanisms traced with concrete evidence, neither GR-confirmed
**Affected Output:** `GenericABCEGNPX.txt`/`RoadbedABCEGNPX.txt` (`saf_abcegnpx_generic`/`saf_abcegnpx_roadbed`)
**Severity:** Low - 7 rows (`saf_abcegnpx_generic`) / 3 rows (`saf_abcegnpx_roadbed`), all row-presence
gaps, no field-value mismatches
**Discrepancy Count:** `saf_abcegnpx_generic`: 5 `only_in_build` + 2 `only_in_legacy`.
`saf_abcegnpx_roadbed`: 2 `only_in_build` + 1 `only_in_legacy` (a subset of generic's 7, same
underlying segments). All 64 `modified` rows in each file are already accounted for by
[Bug 003](./003-saf-gnx-side-ap-point-mismatch.md) (ESRI vs PostGIS side-AP point-in-polygon
disagreement) - this doc covers only the residual row-presence gaps. **None of this is marked
`accounted_for`** - see Status.

## Summary

`data_issues.md`'s `CSCL-SAF-01` previously described these row-count gaps as "not yet traced but
consistent in scale with the already-open, unimplemented SAF-replicant scope." That hypothesis is
retracted. Tracing all 7 `saf_abcegnpx_generic` gap rows to their underlying source records
(`dcp_cscl_commonplace_gdb`, `dcp_cscl_altsegmentdata`) surfaces **two different mechanisms**, not
one:

1. **6 of 7 rows:** dev has a CommonPlace-sourced sub-record prod's SAF lacks - consistent with
   the same "prod's derived output wasn't regenerated from current source" pattern as
   `CSCL-SAF-01` (LGC codes), Bug 010 (`Exception.txt`/`Enders.txt`/`SND.txt`), and Bug 011
   (`gdb_altnames`) - but not GR-confirmed here either.
2. **1 of 7 rows (`9017010`):** originally characterized as "prod has an extra row dev is
   missing" - the same staleness shape, just reversed. **That characterization was wrong.**
   Tracing it to its source row shows dev's exclusion is spec-correct given the source data as it
   stands today; see below.

## Technical Details

### 6 rows: CommonPlace-sourced, dev-only

| segmentid (SAF/LION) | CommonPlace `globalid` | `created_date` / `modified_date` | Shape |
|---|---|---|---|
| `9014017` (face `4496`/`00045`) | `8A3342B5-...` | 2009-05-14 / 2018-08-08 | Same B5SC (`444192`) as prod's row, but prod's version sits under a different face_code/segment_seqnum (`2812`/`00015`) - looks like a renumbering, not a pure addition. `only_in_build`/`only_in_legacy` pair (1 each). |
| `0080484` (face `4872`) | (B5SC `201621`, L side) | 2026-05-05 modified | Extra L-side sub-record, range 0-149, absent from prod entirely. `only_in_build` (1). |
| `0277830` (face `5080`) | `AB0677FD-...` (X) / `EC950080-...` (G) | X: 2026-01-13 / 2026-04-29; G: created 2026-01-13, unmodified | Extra R-side sub-record (B5SC `101867`, range 0-227); the file's 2 L-side rows (B5SC `129445`) are identical on both sides and aren't part of the diff. `only_in_build` (1). |
| `0175232` / `0343566` (faces `1442`/`1444`) | `0E53315A-...`, `D29AD02E-...`, `DD2B7DC7-...` | 2018-2022 range, no 2026 edits | Extra L-side B5SC (`400773`) alongside an R-side B5SC (`400771`) both sides already carry identically; prod is missing the L-side record. `only_in_build` (2, generic only). |

The `0080484`/`0277830` records were edited in 2026 (this build cycle's vintage) - real evidence
current source moved after whatever snapshot prod's SAF reflects. The `9014017`/`0175232`/
`0343566` records were **not** recently edited (2009-2022) - present in source for years, so
their absence from prod isn't obviously explained by a *recent* edit. Both groups are still
absent from prod's SAF today, which is the actual claim being made - the mixed dates mean "prod's
SAF snapshot is old and hasn't caught up" fits better than "this specific edit came in late,"
but neither is confirmed.

### 1 row: NOT staleness - a source-data quality issue our own filter correctly excludes

`9017010`'s `only_in_legacy` row (B5SC `126294`, house range `140-198`/`141-199`) traces to
`dcp_cscl_altsegmentdata`, not CommonPlace. Querying the raw table for this B5SC shows **two**
records:

```
globalid                                 alt_segdata_type  l_low_hn  l_high_hn  r_low_hn  r_high_hn
{4D6300B7-B521-404C-B10A-A450655939B7}   S                 132       138        133       139   <- dev includes this
{A6F964A3-142E-4018-BFC7-01785C6C2F55}   (blank)           140       198        141       199   <- dev excludes this
```

`stg__altsegmentdata_saf.sql` filters `WHERE alt_segdata_type = 'S'` - and per spec (Table 13,
`docs/ETL_V8_02012024.md` ~line 3354), `ALT_SEGDATA_TYPE = 'S'` is literally the definition of
"this is a SAF entry"; `B`/`C`/`R` are the only other valid values (coincident borough-boundary /
continuity / on-off-ramp segments, none of which are SAF entries). A blank value isn't in the
domain at all. Checked citywide: **exactly 1 row** in all of `dcp_cscl_altsegmentdata` (8,043
rows total) has a blank `alt_segdata_type` - this is the row. Our exclusion is spec-correct given
the source data as delivered; prod including it looks like either a legacy-tool quirk (treating
blank as equivalent to `S`) or evidence the field was blanked out in source *after* whatever
snapshot prod's SAF reflects (its `modified_date` is null, which is inconclusive - several other
old rows in this table also carry a null `modified_date` despite having clearly been touched, so
null doesn't reliably mean "never edited" here).

**This is not part of the "6 of 7" staleness claim above** - reclassified as its own, narrower
question: was this row ever validly `S`-typed, and if so, when/why did it go blank?

## Root Cause

Two separate, unconfirmed hypotheses:
1. (6 rows) Same shape as `CSCL-SAF-01`/Bug 010/Bug 011: prod's SAF output not regenerated from
   current `dcp_cscl_commonplace_gdb` each cycle.
2. (1 row, `9017010`) A single malformed/blanked `ALT_SEGDATA_TYPE` value in
   `dcp_cscl_altsegmentdata`, of unknown vintage - not confirmed to be a staleness artifact at
   all, just noted as the one row not covered by our own inclusion filter.

**Neither is GR-confirmed.** This doc stops at "traced to a specific source row/table with a
plausible story," not "resolved."

## How This Could Be Settled

Every source table involved (`dcp_cscl_segment_lgc`, `dcp_cscl_commonplace_gdb`,
`dcp_cscl_altsegmentdata`) carries `created_date`/`modified_date` audit columns. The concrete,
answerable question for GR: **does prod's SAF-extraction job re-run against these tables fresh
each release, or does it work off a cached/carried-forward SAF extract?** If the former, none of
this should be possible - a full re-run against current source would agree with ours. The specific
globalids and dates above are exactly what's needed to check that against whatever date prod's
own SAF-generation job last ran, if that date is recoverable (there's no equivalent to Bug 013's
FileGDB `CreaDate` metadata trick for flat SAF text files - no embedded generation timestamp found
in the delivered files themselves).

## New ETL Implementation

No code change. Not marked `accounted_for` in `qa__diffs_saf_abcegnpx_generic.sql`/
`qa__diffs_saf_abcegnpx_roadbed.sql` - traced but unconfirmed, same convention as `CSCL-SAF-01`.

## Impact Assessment

**Affected Records:** 7 rows citywide (`saf_abcegnpx_generic`), a subset of 3 in
`saf_abcegnpx_roadbed`. All traced to a specific source record; none confirmed as a gap in our
own ETL logic, but none GR-confirmed as prod bugs either.

## References

- `data_issues.md` `CSCL-SAF-01` (same staleness family for 6/7 rows, different source table)
- `docs/prod_bugs/010-featurename-normalizing-tables-stale-accretion.md`
- `docs/prod_bugs/011-altnames-saf-replicant-join-ids.md`
- Spec: `docs/ETL_V8_02012024.md`, Table 13 (`ALT_SEGDATA_TYPE` domain, ~line 3354)
- Diff accounting: `models/etl_dev_qa/diffs/saf/qa__diffs_saf_abcegnpx_generic.sql`,
  `qa__diffs_saf_abcegnpx_roadbed.sql`
- Staging filter: `models/staging/stg__altsegmentdata_saf.sql`
