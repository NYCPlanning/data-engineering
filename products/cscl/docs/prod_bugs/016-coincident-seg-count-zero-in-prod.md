# Bug 016: Prod Publishes `0` for Coincident Segment Count, Which the Spec Forbids

**Status:** Confirmed prod bug, accounted for
**Affected Output:** `BronxLION.dat` (`lion_dat_bronx`)
**Severity:** Low - 1 segment citywide (as far as verified)
**Discrepancy Count:** 1 record (`_lion_key = 211480359093`, segmentid 359093)

## Summary

Segment 359093 (Bronx, a plain `centerline` feature) has `coincident_seg_count` (LION field
L57) = `1` in our output and `0` in prod's real, delivered `BronxLION.dat`. The ETL spec is
explicit that `0` should never appear in this field:

> The output field L57 (Coincident Segment Count) must be populated with the appropriate value
> in every LION output record. If there are no segments coincident with the one being written
> out, the ETL tool will populate its L57 field with the value '1'.

Our `1` is exactly the spec's documented floor for "no coincidences." Prod's `0` isn't a value
the spec sanctions anywhere - it looks like a genuine bug in prod's own output for this one
segment, not a discrepancy in our logic.

## Technical Details

For a `centerline`-feature-type segment, `int__lion.sql` computes L57 as:

```sql
centerline.coincident_seg_count - coalesce(centerline_coincident_subway_or_rail.subway_or_rail_count, 0)
```

`centerline.coincident_seg_count` is a direct passthrough of the CSCL `Centerline` feature
class's own `COINCIDENT_SEG_COUNT` attribute (`stg__centerline.sql`) - per spec, this attribute
itself already encodes the "1 = no coincidences" floor (it's "an integer greater than '1' for
Centerline segments that have coincident segments" - a value of exactly 1 is the class's own
"none" state, not something we compute). For segment 359093:

- `stg__centerline.coincident_seg_count` = `1` (source value, unmodified)
- `int__centerline_coincident_subway_or_rail` has no row for this segment (0 subway/rail overlap)
- Final: `1 - 0 = 1`

Verified prod's actual value directly against the raw delivered file, not just the loaded
comparison table, to rule out a loading artifact:

```
$ grep -n "^.\{10\}0359093" BronxLION.dat
29553:21148002720359093...
$ python3 -c "
with open('BronxLION.dat') as f:
    line = f.readlines()[29552]
print(repr(line[187:188]))  # L57, position 188
"
'0'
```

Byte-for-byte confirmed: prod's real file has a literal `'0'` at that position, not a blank or a
parsing artifact.

## Root Cause

Unconfirmed on prod's side (no access to GR's source `COINCIDENT_SEG_COUNT` history for this
segment), but the spec's own floor rule rules out `0` as ever being intentional. Most likely
explanation, consistent with everything else found this cycle: a data-entry or derived-attribute
quirk specific to this one segment in prod's source, not a general rule to replicate.

## New ETL Implementation

No code change - our value already matches the documented spec exactly. Marked `accounted_for`
in `qa__diffs_lion_dat.sql`, fingerprinted on `_lion_key` (the same one-off convention as Bugs
005/006/007/008), not a general `coincident_seg_count` exclusion - this does not mark every
`coincident_seg_count` diff accounted for, only this specific record.

## Impact Assessment

**Affected Records:** 1 segment, Bronx only, as far as verified against this release. If a
future release shows a similar pattern (dev=1, prod=0) on other segments, re-open this as a
general rule rather than continuing to add one-off keys.

## References

- Spec: `docs/ETL_V8_02012024.md`, "Determining the Coincident Segment Count" (~line 3486)
- Computation: `models/intermediate/int__lion.sql` (L57/`coincident_seg_count` CASE)
- Source passthrough: `models/staging/segments/stg__centerline.sql`
- Diff accounting: `models/etl_dev_qa/diffs/lion_dat/qa__diffs_lion_dat.sql`
- Related (different mechanism, non-centerline segments): `data_issues.md` `CSCL-LION-06`
