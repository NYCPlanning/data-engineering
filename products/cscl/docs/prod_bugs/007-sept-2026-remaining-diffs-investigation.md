# Bug 007: Investigation of the 6 Remaining Unaccounted-For Diffs (Sept 2026)

**Status:** Resolved - see per-record disposition below (item 5&6 still open, needs a decision)
**Affected Output:** LION .dat files (Brooklyn, Queens, Staten Island), RPL, SAF `s_roadbed`/`s_generic`
**Severity:** Low - 6 records, but item 3 exposes a data-integrity risk worth a broader look
**Discrepancy Count:** 6 records (one field/field-group each; the two SAF records share one key)

## Summary

These were the last 6 `accounted_for = false` rows in `qa__diffs_all` as of 2026-09-10. Unlike
[Bug 006](./006-jr-reviewed-manual-diffs-aug-2026.md), this was root-caused from the data rather
than eyeballed against a spreadsheet - each item below was traced from the diff's `comparison_id`
back through the `main` schema's intermediate tables to a specific mechanism.

| comparison_id | table(s) | field(s) | old (prod) &rarr; new (ours) | disposition |
|---|---|---|---|---|
| 304118101066 | lion_dat_brooklyn | `coincident_seg_count` | `2` &rarr; `3` | **Accounted for** - folds into [CSCL-LION-06](../../data_issues.md#cscl-lion-06), open in general |
| 493960174704 | lion_dat_queens | `special_address_flag` | `B` &rarr; ` ` | **Accounted for** - see [Bug 008](./008-saf-flag-leaks-onto-ramp-protosegment.md), prod bug |
| 527570358043 | lion_dat_statenisland | `left_`/`right_` `dynamic_block`, census tract/block basics, assembly/election/school district | various &rarr; all blank | Corrupt source geometry - reported upstream, see item 3 |
| `0167138_0133963` | rpl | `to_nodeid_of_roadbed_segment`, `from_nodeid_of_roadbed_segment` | swapped | **Fixed** - `int__rpl.sql` tiebreak added |
| 415730136981 | saf_s_roadbed, saf_s_generic | `lgc1`, `lgc2` | `01`/` ` &rarr; `03`/`02` | **Still open** - see corrected item 5&6 below |

## 1. `coincident_seg_count`, lion_dat_brooklyn, 304118101066

**segmentid 8101066, borough 3.** `int__noncenterline_coincident_segments` finds three features
sharing the exact same midpoint and length (78.45 ft, distance 0 between all pairs):

| segmentid | feature_type |
|---|---|
| 110499 | centerline |
| 8101066 | rail_and_subway (itself) |
| 8105519 | rail_and_subway |

`int__noncenterline_coincident_segment_count.sql` counts **all** rows in that table for the
segment (`COUNT(*) ... WHERE distance < .001`), which by design includes the self-match - the
`COALESCE(sm_count, 1)` fallback elsewhere in the same query implies "you always count yourself"
is intentional, not a bug. So our count of 3 = self + 2 distinct coincident neighbors.

Prod's count of 2 implies it only recognizes **one** external neighbor here, not two. Since
110499 (centerline) and 8105519 (rail_and_subway) are both genuinely coincident and distinct
segment IDs, there's no join or geometry defect on our side to point to - this looks like the
same open, general question already tracked as
[CSCL-LION-06](../../data_issues.md#cscl-lion-06) ("Coincident segments... What would settle it:
a decision with GR on how we handle them"), most likely involving how prod treats two
same-feature-type (`rail_and_subway`) duplicates stacked at one location.

**Resolution:** Marked `accounted_for = true` in `qa__diffs_lion_dat.sql`, fingerprinted on this
exact `_lion_key` (not a general field-pattern rule - other `coincident_seg_count` diffs stay
open). A note pointing back here was added to `data_issues.md`'s CSCL-LION-06 entry, which
remains **Open** in general.

Query used to pull the three coincident geometries into one row (e.g. to open in QGIS):

```sql
SELECT
    s.segmentid,
    s.boroughcode,
    s.geom AS this_segment_geom,
    s.feature_type AS this_segment_feature_type,
    c.geom AS coincident_centerline_geom,
    r.geom AS coincident_rail_geom
FROM int__primary_segments AS s
CROSS JOIN LATERAL (
    SELECT geom FROM int__primary_segments WHERE segmentid = 110499
) AS c
CROSS JOIN LATERAL (
    SELECT geom FROM int__primary_segments WHERE segmentid = 8105519
) AS r
WHERE s.segmentid = 8101066;
```

All three `geom` values are identical (`ST_Distance` 0 between every pair) - the three features
are exactly stacked on top of each other.

## 2. `special_address_flag`, lion_dat_queens, 493960174704

**segmentid 174704, borough 4, face_code 9396.** This segmentid has two representations in
`int__lion`:

| lionkey | source_table | face_code | alt_segdata_type | special_address_flag |
|---|---|---|---|---|
| 4560207075 | centerline | 5602 | (n/a) | `B` |
| 4939600450 | altsegmentdata | 9396 | `R` | *(blank)* |

The diff is about the second row (face_code 9396, our target key). `stg__altsegmentdata_saf`
and `dcp_cscl_addresspoints` both carry a real `saftype = 'B'` record for segmentid 174704 - but
[design_doc.md](../../design_doc.md#special-address-flag) quotes the legacy ETL spec verbatim:

> If the proto-segment is of the combined on-off ramp type (ALT_SEGDATA_TYPE = R): It will not
> have any appurtenant SAF type A, B, C, D, E, F, O, P, S or V data, since these are all
> address-related and ramps will never have addresses.

`int__lion.sql`'s suppression `CASE` (lines 172-178) implements exactly this: it blanks
`special_address_flag` when the row is an `altsegmentdata` protosegment whose `alt_segdata_type`
isn't `'B'` and the joined SAF record is one of the address-related types (which includes `'B'`,
confusingly a SAF type distinct from the `ALT_SEGDATA_TYPE = 'B'` mentioned above). Since this
row's `alt_segdata_type = 'R'` (on/off-ramp), our code correctly nulls the flag per spec.

Prod instead prints `B` on the ramp record - which the documented rule says should never happen
(ramps have no addresses). This looks like a genuine legacy bug: prod appears to leak the SAF
flag from the sibling centerline record (4560207075) onto its ramp protosegment (4939600450)
instead of suppressing it.

**Resolution:** Written up as [Bug 008](./008-saf-flag-leaks-onto-ramp-protosegment.md) and
marked `accounted_for = true` in `qa__diffs_lion_dat.sql`, fingerprinted on this `_lion_key`.
Still worth confirming with GR, and worth checking whether other `ALT_SEGDATA_TYPE = 'R'`
protosegments citywide show the same leak (see Bug 008's Impact Assessment).

## 3. LION district/census fields, lion_dat_statenisland, 527570358043

**segmentid 358043, borough 5.** All the changed fields (`left_`/`right_` `dynamic_block`,
`assembly_district`, `election_district`, `school_district`, and every census tract/block
"basic" vintage) are derived by `int__segment_atomicpolygons.sql` via
`ST_Within(offset_point, atomic_polygon.geom)`. For this segment, **both** `left_atomicid` and
`right_atomicid` come back `NULL` - the join fails on both sides simultaneously, which is why
every field derived from it went blank at once.

The offset points themselves are garbage:

```
left_offset_point:  POINT(-4.168431851136065 46.509119105993214)
right_offset_point: POINT(-8.12188914583555 45.90069867653063)
```

These aren't remotely close to State Plane 2263 coordinates for Staten Island (should be ~900k
E / ~140k N). Tracing back: the raw `stg__centerline` geometry for segmentid 358043 is an
`ST_MultiCurve` whose `ST_Length` already reports **2,985,404 ft (~565 miles)** for what should
be an ordinary short street segment, and after `linearize()`/`ST_CurveToLine` it balloons into a
line with **96,850 vertices**. The raw WKT shows why - one `CIRCULARSTRING` is defined with the
control point:

```
CIRCULARSTRING(939189.2462158203 144622.6864013672, 541902.8247159402 541909.1079012473,
               0 6.283185307179586, ...)
```

`6.283185307179586` is `2*pi()` to 15 digits, sitting in a Y-coordinate slot. This is almost
certainly a radians value that leaked into a State-Plane-feet coordinate field somewhere upstream
in the source system, producing a mathematically-degenerate circular arc. `ST_CurveToLine`
renders that arc as an enormous, self-crossing loop; the segment's `midpoint` and offset points
inherit that corruption and land far outside the city, so they can't fall inside any atomic
polygon on either side.

This is the same general failure mode already tracked as
[CSCL-LION-07](../../data_issues.md#cscl-lion-07) (center-of-curvature errors from `linearize()`
coercing curves) and [CSCL-DISTRICTS-02](../../data_issues.md#cscl-districts-02), but a much more
severe instance - instead of a small positional shift, it zeroes out an entire segment's
location-derived attributes. Given the literal `2*pi()` constant, this looks like corrupt source
data rather than something fixable in our SQL.

**Source record to report upstream.** The corrupt geometry lives on the raw `dcp_cscl_centerline`
row itself (from the `Centerline` layer of `ETL Working GDB.gdb.zip`, ingested as-is - no
transformation of ours produces the bad coordinate):

| Field | Value |
|---|---|
| Table | `dcp_cscl_centerline` (CSCL `Centerline` feature class) |
| `globalid` | `{6A2A395A-9E9A-4C94-A86F-35782DDC45B0}` |
| `ogc_fid` | 183041 |
| `segmentid` | 358043 |
| `physicalid` | 206875 |
| `boroughcode` | 5 (Staten Island) |
| `created_by` / `created_date` | J_Rivas / 2022-05-05 13:26:54+00 |
| `modified_by` / `modified_date` | J_Rivas / 2025-12-23 11:50:10+00 |

That `modified_date` (2025-12-23) is well after `created_date` (2022-05-05), so this record was
edited in CSCL - most plausibly when/around whatever edit introduced the bad `2*pi()` control
point. GR/whoever owns Centerline editing should be able to pull this exact GlobalID up in the
CSCL editor and see the malformed curve directly.

**Recommendation:** Report this record (by GlobalID above) to GR as a bad-geometry edit that
needs re-drawing. Separately, worth a defensive sanity check in our own pipeline (e.g. flag or
reject centerline geometries whose linearized length is wildly disproportionate to their vertex
count) so one corrupt curve can't silently null out a segment's LION district fields in the
future - nothing catches that today.

## 4. RPL node swap, `0167138_0133963`

**generic_segmentid 0167138, roadbed_segmentid 0133963.** `int__lion` has two rows for segmentid
133963:

| lionkey | source_table | from_nodeid | to_nodeid |
|---|---|---|---|
| 3073605387 | centerline | 103190 | 25931 |
| 3124801205 | altsegmentdata | 25931 | 103190 |

Same two endpoints, opposite direction - a reversed protosegment, the same general phenomenon as
[CSCL-LION-01](../../data_issues.md#cscl-lion-01) (though this segmentid isn't one of the 5
already listed there).

`int__rpl.sql`'s `lion` CTE does:

```sql
SELECT DISTINCT ON (segmentid) segmentid, segment_type, from_nodeid, to_nodeid, geom, midpoint
FROM {{ ref("int__lion") }}
ORDER BY segmentid
```

There's no tiebreaker beyond `segmentid`, so when a segmentid has multiple `int__lion` rows,
Postgres keeps whichever one it happens to encounter first in scan order - undefined, and here it
picked the reversed `altsegmentdata` row, flipping `from_nodeid_of_roadbed_segment` and
`to_nodeid_of_roadbed_segment` relative to prod.

This isn't a one-off: **1,465 segmentids** have more than one row in `int__lion`, and of those,
**24 segmentids** have genuinely conflicting `(from_nodeid, to_nodeid)` pairs across their
representations - i.e. 24 places where this `DISTINCT ON` is silently picking an arbitrary
direction. This RPL diff is one manifestation of that broader ambiguity; there may be others not
yet surfaced (RPL only touches segmentids that appear in
`dcp_cscl_roadbed_pointer_list`).

**Resolution:** Fixed. `int__rpl.sql`'s `lion` CTE now orders by
`segmentid, (source_table = 'centerline') DESC`, so the centerline row wins deterministically
whenever one exists. Verified directly against segmentid 133963 post-fix:

```sql
SELECT DISTINCT ON (segmentid) segmentid, source_table, from_nodeid, to_nodeid
FROM int__lion
WHERE segmentid = 133963
ORDER BY segmentid, (source_table = 'centerline') DESC;
--  segmentid | source_table | from_nodeid | to_nodeid
-- -----------+--------------+-------------+-----------
--     133963 | centerline   |      103190 |     25931   <- matches prod
```

Worth re-running the full diff after this lands to see if any of the other 24 ambiguous
segmentids (of the 1,465 with duplicate `int__lion` rows) affect other RPL records.

## 5 & 6. SAF `lgc1`/`lgc2`, 415730136981 (s_roadbed and s_generic) - CORRECTED

**An earlier version of this doc explained this as source-data drift ("prod's snapshot predates
a CSCL edit"). That explanation doesn't hold up and has been retracted** - both ETLs are meant to
run against the exact same release inputs, and `data_library_version` turned out to be a
whole-table ingest stamp, not a per-record edit timestamp (see below). The real mechanism is a
gap in our own QA tooling, not the CSCL source data.

**segmentid 136981, borough 4, face_code 1573.** Same `comparison_id` and field change appears
identically in both `saf_s_roadbed` and `saf_s_generic`, because both derive `lgc1`/`lgc2` from
the same `int__lgc.sql` &rarr; `int__streetcode_and_facecode.sql` pipeline (same pattern noted in
[Bug 006](./006-jr-reviewed-manual-diffs-aug-2026.md) for a different segment).

**Why "source drift" was wrong.** `recipe.lock.yml` shows every single dataset from
`ETL Working GDB.gdb.zip` - including `dcp_cscl_segment_lgc` - resolved to the identical archive
timestamp `2026-06-17T16:36:51.341725-04:00`. All 319,219 rows in `dcp_cscl_segment_lgc` carry
that same `data_library_version`. That rules out both of the theories that timestamp seemed to
support: it's not evidence this segment's LGC list was edited recently (it's just when the whole
GDB was archived), and it's not a per-dataset version-fallback skew either (`missing_versions_strategy:
find_latest` in `recipe.yml` - flagged there with a `# TODO - they should all match` - doesn't
actually bite here, since every layer in this GDB is one atomic file/ingest event).

**What's actually going on.** `production_outputs.saf_s_roadbed`/`saf_s_generic` aren't a fresh
re-run of legacy code against our current recipe - they're real prod export files GR provided,
loaded once via `poc_validation/prod_data_loader.py load -v {version} -d {dataset_name}`
(see `README.md`'s "Setup" section). That script's generic `load` command (used for SAF, unlike
`load_citywide_lion`/`load_previous_ldf_header`) **never calls `already_loaded()` or
`record_load()`** - it doesn't touch `load_log` at all:

```python
@app.command("load")
def _load(...):
    ...
    load_datasets(datasets, local_folder)   # no already_loaded check, no record_load call
```

Consistent with that, querying `production_outputs.load_log` today only shows 4 rows - all
LION/LDF, none for any SAF table:

```
table_name                  | version | loaded_at
previous_ldf_header         | 26a     | 2026-08-31...
previous_citywide_lion_dat  | 26a     | 2026-08-31...
ldf_header                  | 26b     | 2026-08-31...
ldf_base                    | 26b     | 2026-08-31...
```

So there's no record anywhere of which release's prod files are actually sitting in
`production_outputs.saf_s_roadbed`/`saf_s_generic` right now, or whether they were ever reloaded
for 26b. Given our current 26b `dcp_cscl_segment_lgc` extract has exactly two rows for segmentid
136981 (`03`/preferred, `02`) and **no** `01` anywhere in the table, the most likely explanation
is that the loaded SAF prod tables predate 26b and were never refreshed - not that our join or
ranking logic is wrong (the ranking - preferred first, then `lgc` ascending - is straightforward
and matches [design_doc.md's description](../../design_doc.md) of the Segment_LGC aggregation).

**Recommendation:**
1. Re-pull the 26b SAF prod files and reload them: `python3 poc_validation/prod_data_loader.py load -v 26b -d saf_s_roadbed saf_s_generic` (and likely the other SAF/non-LION-LDF datasets too, since they share the same untracked `load` path).
2. Re-run the diff after reloading - if the mismatch disappears, that confirms the stale-load theory and no code or `accounted_for` change is needed here.
3. Consider wiring `already_loaded`/`record_load` into the generic `load` command the way `load_citywide_lion` already works, so a stale `production_outputs` table can't silently pass for "the same inputs" again.

Not marking `accounted_for` yet - this needs the reload-and-recheck step above first.

## References

- Diff accounting: `models/etl_dev_qa/diffs/lion_dat/qa__diffs_lion_dat.sql`,
  `models/etl_dev_qa/diffs/rpl/qa__diffs_rpl.sql`,
  `models/etl_dev_qa/diffs/saf/qa__diffs_saf_s_roadbed.sql`,
  `models/etl_dev_qa/diffs/saf/qa__diffs_saf_s_generic.sql`
- Related: [CSCL-LION-01](../../data_issues.md#cscl-lion-01),
  [CSCL-LION-06](../../data_issues.md#cscl-lion-06),
  [CSCL-LION-07](../../data_issues.md#cscl-lion-07),
  [CSCL-DISTRICTS-02](../../data_issues.md#cscl-districts-02),
  [Bug 001](./001-boe-lgc-pointer-two-digit-codes.md),
  [Bug 006](./006-jr-reviewed-manual-diffs-aug-2026.md),
  [Bug 008](./008-saf-flag-leaks-onto-ramp-protosegment.md)
- SAF/ALT_SEGDATA_TYPE spec quoted from [design_doc.md § Special Address Flag](../../design_doc.md#special-address-flag)
- Prod-load tooling: `poc_validation/prod_data_loader.py`, `production_outputs.load_log`
