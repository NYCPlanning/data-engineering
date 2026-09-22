# Bug 017: `saf_abcegnpx_*`'s Row-Count Diffs Are the Same Stale-Prod Pattern as CSCL-SAF-01, Not SAF-Replicant Scope

**Status:** Confirmed prod bug (stale source snapshot), not fixable on our side
**Affected Output:** `GenericABCEGNPX.txt`/`RoadbedABCEGNPX.txt` (`saf_abcegnpx_generic`/`saf_abcegnpx_roadbed`)
**Severity:** Low - 7 rows (`saf_abcegnpx_generic`) / 3 rows (`saf_abcegnpx_roadbed`), all row-presence
gaps, no field-value mismatches
**Discrepancy Count:** `saf_abcegnpx_generic`: 5 `only_in_build` + 2 `only_in_legacy`.
`saf_abcegnpx_roadbed`: 2 `only_in_build` + 1 `only_in_legacy` (a subset of generic's 7, same
underlying segments). All 64 `modified` rows in each file are already accounted for by other
documented bugs (Bug 006, CSCL-SAF-01) - this doc covers only the residual row-presence gaps.

## Summary

`data_issues.md`'s `CSCL-SAF-01` previously described these row-count gaps as "not yet traced but
consistent in scale with the already-open, unimplemented SAF-replicant scope." That hypothesis is
retracted: tracing all 7 `saf_abcegnpx_generic` gap rows to their underlying segments shows the
same "prod's derived SAF output wasn't fully regenerated from current source" pattern documented
for `saf_s_*`'s LGC codes (`CSCL-SAF-01`), `Exception.txt`/`Enders.txt`/`SND.txt` (Bug 010), and
`gdb_altnames` (Bug 011) - not a missing feature on our side.

## Technical Details

Four segments account for all 7 `saf_abcegnpx_generic` gap rows:

| segmentid | face_code/segment_seqnum | Shape |
|---|---|---|
| `9014017` | dev `4496`/`00045` vs prod `2812`/`00015` | Same B5SC (`444192`), same house-number range (all zero/unaddressed) - dev's face_code and segment_seqnum have been renumbered relative to prod's. `only_in_build`/`only_in_legacy` pair (1 each). |
| `0080484` | dev face `4872` | Dev has an extra L-side B5SC `201621` (range 0-149) that prod has no row for at all. `only_in_build` (1). |
| `0277830` | dev face `5080` | Dev has an extra R-side B5SC `101867` (range 0-227) that prod has no row for; the file's 2 L-side rows (B5SC `129445`) are identical in both and aren't part of the diff. `only_in_build` (1). |
| `0175232` / `0343566` | dev faces `1442`/`1444` | Dev has an extra L-side B5SC (`400773`) alongside the R-side B5SC (`400771`) that both sides already carry identically; prod is missing the L-side record. `only_in_build` (2, generic only - not present in the roadbed diff). |
| `9017010` | face `5495`, seqnum `02019` | The one row running the *other* direction: **prod** has an extra B5SC `126294` house-number sub-range (`140-198`/`141-199`) that dev doesn't produce; a second sub-range on the same B5SC (`132-138`/`133-139`) matches on both sides. `only_in_legacy` (1, in both generic and roadbed). |

Six of the seven rows are "dev has a newer/renumbered SAF sub-record that prod's snapshot lacks" -
directly analogous to `CSCL-SAF-01`'s LGC finding (current source has moved on, prod hasn't). The
seventh (`9017010`) runs the opposite direction - prod carries an address sub-range dev's current
source no longer has - but is the same underlying phenomenon as Bug 011's `gdb_altnames` gap
(prod retaining fragments tied to an old source snapshot that current source has since
consolidated or removed), just manifesting as a "prod-only" row instead of a "dev-only" one.

## Root Cause

Same as `CSCL-SAF-01`, Bug 010, and Bug 011: prod's SAF output is derived from source tables
(here, the address-range/B5SC source feeding `int__saf_abcep`/`int__saf_gnx`) that aren't fully
regenerated each release cycle. Not confirmed on GR's side; inferred from the pattern matching
three independently-discovered instances of the same staleness signature this cycle.

## New ETL Implementation

No code change - our output is correct given current source data in every one of the 7 cases
traced. Not marked `accounted_for` in `qa__diffs_saf_abcegnpx_generic.sql`/
`qa__diffs_saf_abcegnpx_roadbed.sql`: unlike `CSCL-SAF-01`'s LGC pattern, these rows don't share a
single field-level fingerprint (they're a mix of `only_in_build`/`only_in_legacy` across several
distinct segments) small enough in count (7 and 3) that a general rule isn't warranted - kept as
visible, individually-traceable diffs rather than an opaque blanket exclusion.

## Impact Assessment

**Affected Records:** 7 rows citywide (`saf_abcegnpx_generic`), a subset of 3 in
`saf_abcegnpx_roadbed`. All traced; none indicate a gap in our own ETL logic.

## References

- `data_issues.md` `CSCL-SAF-01` (same staleness family, different source table)
- `docs/prod_bugs/010-featurename-normalizing-tables-stale-accretion.md`
- `docs/prod_bugs/011-altnames-saf-replicant-join-ids.md`
- Diff accounting: `models/etl_dev_qa/diffs/saf/qa__diffs_saf_abcegnpx_generic.sql`,
  `qa__diffs_saf_abcegnpx_roadbed.sql`
