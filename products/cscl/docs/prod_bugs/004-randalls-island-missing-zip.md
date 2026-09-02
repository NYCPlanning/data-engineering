# Bug 004: Randall's Island Segments Missing Zip Code in Production

**Status:** GR-confirmed production bug - not recreating it
**Affected Output:** LION .dat files (Manhattan)
**Severity:** Low - 10 records, all in the middle of Randall's Island
**Discrepancy Count:** 10 records (segmentids 0246013, 0246014, 0246016, 0246017, 0246018,
0246019, 0246021, 0246022, 0279055, 0279056), all borough 1 (Manhattan)

## Summary

Ten non-street-feature (NSF) segments in the middle of Randall's Island - an area that is
entirely a single zip code, 10035 - come back blank for `l_zip`/`r_zip` in production. This
new ETL correctly populates `10035` for them via the standard non-centerline zip spatial join.
GR confirmed this is a legacy production bug and asked us not to reproduce it.

This is also tracked as `CSCL-LION-02` in `data_issues.md`; this doc exists so the
discrepancy can be formally marked `accounted_for` in the diff tooling the same way the other
numbered bugs are.

## Technical Details

### How zip is normally populated for non-centerline segments

**Location:** `models/intermediate/int__lion.sql`

```sql
CASE
    WHEN segment_locational_status.borough_boundary_indicator = 'L' THEN NULL
    WHEN segments.source_table = 'altsegmentdata' AND ...boundary_indicator = 'R'
        THEN proto.zipcode
    WHEN segments.feature_type = 'centerline' THEN centerline.l_zip
    ELSE coalesce(primary_centerline.l_zip, zips.l_zip)
END AS l_zip
```

(mirrored for `r_zip`). Per `design_doc.md`'s Adjacent Polygons section, this spatial-join
fallback ("Zip Codes, only for non-centerline segments") is deliberate: non-centerline segments
(NSF, shoreline, rail/subway, most protosegments) don't carry a `ZIP` attribute in their source
layer the way Centerline features do, so the ETL falls back to a point-in-polygon join against
the DCP ZipCode layer (`models/intermediate/adjacent_polygons/int__segment_zipcodes.sql`),
using offset points just off the segment's midpoint.

### Example: segment 246013

`_lion_key = 100640246013`, `feature_type_code = 8` (NSF, "physical boundary such as a
cemetery wall" - this one runs through the middle of Randall's Island). The spatial join against
the ZipCode layer correctly resolves both `l_zip` and `r_zip` to `10035`, since the entire area
is one zip code. Production leaves both blank for this segment and the nine others listed
above.

## Root Cause

Unknown/not our bug. GR reviewed this discrepancy and confirmed it is a bug in the legacy
production ETL - these ten segments simply never got a zip code assigned there, for reasons
internal to GR's tooling that we don't have visibility into. There is no equivalent gap in this
ETL's logic to fix.

## New ETL Implementation

No code change - the new ETL's non-centerline zip spatial join
(`int__segment_zipcodes.sql` + the `ELSE` branch in `int__lion.sql`) is working as designed for
these segments. GR explicitly asked that we not try to recreate production's omission here.

## Impact Assessment

**Affected Records:** 10 segments, all in Manhattan, all in the interior of Randall's Island.

**Change Pattern:** `l_zip`/`r_zip` change from blank (production) to `10035` (this ETL).
Several of these segments also show an unrelated `segment_seqnum` diff (already covered by the
general segment_seqnum discrepancy rule in `qa__diffs_lion_dat.sql`).

**Recommendation:** Marked as `accounted_for = true` in `qa__diffs_lion_dat.sql`. The match is
fingerprinted on the *value transition* - `l_zip` and `r_zip` both going from blank to
specifically `10035` - rather than on these ten segmentids directly. Segmentids can be
renumbered or (per CSCL's history) reused across LION editions, so pinning to specific IDs
risks either going silently stale (a renumbered segment stops matching - safe, just noisy) or,
worse, wrongly sweeping in an unrelated future segment that happens to reuse one of these IDs.
Requiring the exact blank -> `10035` transition on both sides is specific enough to Randall's
Island that it shouldn't false-positive on an unrelated discrepancy, without depending on any
particular segmentid staying stable.

## References

- Also tracked as `CSCL-LION-02` in `data_issues.md`
- Zip join logic: `models/intermediate/int__lion.sql`,
  `models/intermediate/adjacent_polygons/int__segment_zipcodes.sql`
- Diff accounting: `models/etl_dev_qa/diffs/lion_dat/qa__diffs_lion_dat.sql`
