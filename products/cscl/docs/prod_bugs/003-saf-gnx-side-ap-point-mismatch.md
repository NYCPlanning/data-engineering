# Bug 003: SAF GNX Side-AP Fields Differ Near Atomic Polygon Boundaries (ESRI vs. PostGIS Point-in-Polygon)

**Status:** Understood - not a bug in the new ETL, expected floating-point divergence
**Affected Output:** SAF ABCEGNPX (Generic and Roadbed) - `side_ap`, `side_borough_code`,
`side_ct2020_basic`, `side_ct2020_suffix` (GNX-only fields)
**Severity:** Low - affects only CommonPlace points that sit essentially on an Atomic Polygon
boundary
**Discrepancy Count:** Small, expected to grow slowly over time as new CommonPlace points are
added near AP boundaries. See `qa__diffs_saf_abcegnpx_generic`/`_roadbed` for the current count.

## Summary

For SAF records sourced from CommonPlace (`saftype` `G`, `N`, or `X`), the `side_ap` field (and
the associated `side_borough_code`/`side_ct2020_basic`/`side_ct2020_suffix` fields) is assigned
by finding which Atomic Polygon spatially contains the CommonPlace feature's point location -
not by any relationship to the segment the SAF record is attached to. When that point sits
essentially exactly on the shared boundary between two Atomic Polygons, PostGIS's point-in-
polygon test can resolve it into a different polygon than the legacy ESRI-based pipeline did,
the same class of disagreement documented in
[Bug 002](./002-police-geo-centroid-mismatch.md).

## Technical Details

### The join logic

**Location:** `models/intermediate/saf/int__saf_gnx.sql`

```sql
SELECT
    ...
    atomic_polygons.borocode AS side_borough_code,
    atomic_polygons.censustract_2020_basic AS side_ct2020_basic,
    atomic_polygons.censustract_2020_suffix AS side_ct2020_suffix,
    RIGHT(atomic_polygons.atomicid, 3) AS side_ap,
    ...
FROM saf
LEFT JOIN commonplace ON saf.saf_globalid = commonplace.globalid
...
LEFT JOIN atomic_polygons ON ST_CONTAINS(atomic_polygons.geom, commonplace.geom)
WHERE saf.saftype IN ('G', 'N', 'X')
```

The segment-level fields (`face_code`, `segment_seqnum`, etc.) come from a separate,
attribute-based join (`saf.saf_globalid = commonplace.globalid`). The `side_*` fields are
purely a function of a single, unconditional `ST_CONTAINS` test between the CommonPlace point
and the Atomic Polygon layer - there's no fallback logic (unlike ThinLION's centroid join,
which tries a fallback point if the first one fails `ST_WITHIN`), since a CommonPlace point is
never expected to fall outside every Atomic Polygon under normal circumstances.

### Why it still disagrees with production

Same root cause as Bug 002: ESRI's and PostGIS's point-in-polygon implementations don't
necessarily agree on which polygon a point belongs to when that point sits essentially exactly
on the shared boundary between two polygons. For the vast majority of CommonPlace points this
makes no difference; it only matters for the rare point that happens to fall on (or a hair's
width from) an Atomic Polygon boundary.

### Example: SAF record for segment 301571 (Bronx)

`_saf_key = 205230301571`, CommonPlace `placeid 1026928` (`globalid {77C6EC3B-6CB9-44A4-912F-B9ED08654367}`),
point `POINT(1003134.750793457 233617.24578857422)`:

```
                     | side_ap | side_ct2020_basic | side_ct2020_suffix | distance from point
---------------------+---------+--------------------+---------------------+---------------------
new (this ETL)       | 905     | 51                 | (blank)             | 0.0 ft (contained)
old (production)     | 913     | 19                 | 01                  | 0.0001 ft (~1/1000 in)
```

Atomic Polygons `2005100905` and `2001901913` meet almost exactly at this point - PostGIS's
`ST_CONTAINS` places it inside `905`; the legacy pipeline placed it in `913`. Neither answer is
"wrong" in the sense of a coding error; the point is genuinely ambiguous at that precision.

## Root Cause

Floating-point/algorithmic divergence between ESRI's and PostGIS's point-in-polygon
implementations, surfaced only for the small set of CommonPlace points that fall essentially
exactly on an Atomic Polygon boundary. Same underlying cause as
[Bug 002](./002-police-geo-centroid-mismatch.md), applied to a different join (a raw
`ST_CONTAINS` on a source point rather than a computed centroid-with-fallback).

## New ETL Implementation

No code change is warranted - `models/intermediate/saf/int__saf_gnx.sql` is a correct,
straightforward implementation of "which Atomic Polygon contains this point." Matching ESRI's
point-in-polygon computation bit-for-bit is out of scope.

## Impact Assessment

**Affected Records:** A small number of SAF ABCEGNPX records whose CommonPlace point sits on an
Atomic Polygon boundary. All observed cases so far are `side_ap`/`side_ct2020_basic`/
`side_ct2020_suffix` (and, where the AP also crosses a borough line, `side_borough_code`)
changing together, since they're all derived from the same `ST_CONTAINS` match.

**Recommendation:** Marked as `accounted_for = true` in `qa__diffs_saf_abcegnpx_generic.sql`
and `qa__diffs_saf_abcegnpx_roadbed.sql` when the only changed fields are a subset of
`side_ap`, `side_borough_code`, `side_ct2020_basic`, `side_ct2020_suffix`.

## References

- Join logic: `models/intermediate/saf/int__saf_gnx.sql`
- Related issue: [Bug 002](./002-police-geo-centroid-mismatch.md)
- Diff accounting: `models/etl_dev_qa/diffs/saf/qa__diffs_saf_abcegnpx_generic.sql`,
  `models/etl_dev_qa/diffs/saf/qa__diffs_saf_abcegnpx_roadbed.sql`
