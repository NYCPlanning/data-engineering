# Bug 005: Doubly-Reversed Protosegments Not Reversed in Production

**Status:** GR-confirmed production bug - not recreating it
**Affected Output:** LION .dat files (Bronx, Brooklyn)
**Severity:** Low - 5 known segments
**Discrepancy Count:** 5 segments (segmentids 0016558, 0343093, 0343094, 0343095, 0343096)

## Summary

Five segments each have **two** reversed protosegments (`altsegmentdata.reversed = true`)
pointing back at the same geometry-modeled segment. Production's legacy ETL processes
protosegments one at a time and updates the underlying segment's fields as it goes; when two
reversed protosegments both touch the same segment, their flips collide and cancel out,
leaving production's copy of the segment unreversed. This ETL applies each protosegment's
reversal independently and correctly, so our output is reversed (correct) where prod's is not.

This is also tracked as `CSCL-LION-01` in `data_issues.md`, which has the full narrative; this
doc exists so the discrepancy can be formally marked `accounted_for` in the diff tooling the
same way the other numbered bugs are.

## Technical Details

**Location:** legacy CSCL ETL (protosegment processing order); no equivalent step exists in
this pipeline since protosegment reversal is applied per-record rather than by mutating a
shared segment representation.

A reversed protosegment flips many fields relative to the underlying segment: `from_x`/`to_x`,
`from_y`/`to_y`, `from_nodeid`/`to_nodeid`, `left_dynamic_block`/`right_dynamic_block`, and the
left/right census tract and block fields (2000/2010/2020) all swap. A single reversed
protosegment reversing these fields is expected, correct behavior. The bug specifically
requires **two or more** reversed protosegments sharing a segmentid - confirmed by checking
`stg__altsegmentdata_proto`: of 24 distinct segmentids with at least one `reversed = true`
protosegment, only these 5 have exactly 2, and those 5 are exactly the segments in this list.

### How to identify these segments in source data

```sql
SELECT segmentid
FROM stg__altsegmentdata_proto
WHERE reversed = true
GROUP BY segmentid
HAVING count(*) >= 2
```

This is the mechanism-derived identifier; the hardcoded list below is used instead only because
it's been independently verified stable since 2026-03-02 (originally documented in
`products/cscl/README.md`, later moved verbatim into `data_issues.md`) - about six months as of
this writing, with no changes to the segmentid list across that move. If this ever needs
re-deriving (e.g. a future release adds a new doubly-reversed segment), the query above will
find it.

## Root Cause

Legacy production ETL processing-order bug: protosegments are processed sequentially against a
shared, mutable representation of the geometry-modeled segment, so multiple reversed
protosegments touching the same segment flip its fields back and forth rather than each being
applied independently.

## New ETL Implementation

No code change - each protosegment's fields are derived independently per record rather than
by mutating shared state, so this pipeline doesn't have the ordering dependency that causes
prod's bug. GR confirmed our (reversed) output is correct and asked us not to recreate prod's
omission.

## Impact Assessment

**Affected Records:** 5 segments (Bronx and Brooklyn), each showing a full swap of
directional/left-right fields.

**Recommendation:** Marked as `accounted_for = true` in `qa__diffs_lion_dat.sql` for exactly
these five segmentids.

## References

- Also tracked as `CSCL-LION-01` in `data_issues.md`
- Diff accounting: `models/etl_dev_qa/diffs/lion_dat/qa__diffs_lion_dat.sql`
