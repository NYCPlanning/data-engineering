# CSCL Data Issues

Every known difference between our output and production, plus source-data problems we've
hit. One entry per issue, with a stable ID so code comments, dbt descriptions and issues
can point at it and stay valid as this file is reordered.

This file holds our **understanding** of each issue — what we see, what we currently
believe, and what would settle it. It is not a work tracker: no owners, no dates, no
priorities. Those belong in GitHub issues, which should link back here rather than restate
the analysis. An entry outlives the issue that closes it — "GR confirmed this is fine" is
knowledge we need in five releases' time.

The spec for what each output *should* contain lives in
[design_doc.md](./design_doc.md); this file only records where we diverge from it or from
prod. Some of these are also tracked loosely in GR's
[discrepancy log](https://nyco365.sharepoint.com/:w:/r/sites/NYCPLANNING/itd/edm/Shared%20Documents/DOCUMENTATION/GRU/CSCL/ETL/DE%20Pipeline%20-%20Project%20Tracking/Data%20Discrepancy%20Tracking/LION%20Flat%20Files%20%E2%80%93%20Data%20DiscrepancyIssue%20Tracking.docx?d=w60907e50f8044bd9bffe2508a299035f&csf=1&web=1&e=aZ59n8).

## Status vocabulary

| Status | Meaning |
|---|---|
| **Open** | Unexplained, or explained but undecided. Needs work or a decision. |
| **Accepted** | Understood and deliberately not changing. Usually GR confirmed. |
| **Watch** | Was resolved, can recur. Check each release. |

**Last verified** is the product version the entry was last checked against — not when it
was written. If it's stale, treat the entry as a hypothesis rather than a finding.

## Index

| ID | Output | Issue | Status | Last verified |
|---|---|---|---|---|
| [CSCL-LION-01](#cscl-lion-01) | LION | Doubly-reversed proto segments | Accepted | 26b |
| [CSCL-LION-02](#cscl-lion-02) | LION | 10 rows missing zip code 10035 in prod | Accepted | 26b |
| [CSCL-LION-03](#cscl-lion-03) | LION | Curve flag `I` where prod is blank | Accepted | 26b |
| [CSCL-LION-04](#cscl-lion-04) | LION | BOE LGC pointer wrong for 568 records in prod | Accepted | 26b |
| [CSCL-LION-05](#cscl-lion-05) | LION | Nonstreet feature segment sequence numbers | Accepted | 26b |
| [CSCL-LION-06](#cscl-lion-06) | LION | Coincident segments | Accepted | 26b |
| [CSCL-LION-07](#cscl-lion-07) | LION | Center of curvature | Watch | 26a |
| [CSCL-LION-08](#cscl-lion-08) | LION | `VIntersect` hardcoded null in `gdb_node` | Accepted | 26b |
| [CSCL-LION-09](#cscl-lion-09) | LION | `node_stname` abbreviation (fixed) / `gdb_altnames` `Join_ID` gap (open) | Open | 26b |
| [CSCL-LION-10](#cscl-lion-10) | LION | `LegacyID` real-value mismatches on ~2% of segments | Accepted | 26b |
| [CSCL-LION-11](#cscl-lion-11) | LION | `segment_locational_status` uses 2010, not 2020, census tracts | Accepted | 26b |
| [CSCL-LION-12](#cscl-lion-12) | LION | Two GR/GSS-flagged discrepancies (133963 traffic_direction, 241972 SAF) | Open | 26b |
| [CSCL-DISTRICTS-01](#cscl-districts-01) | District gdb | GDAL `organizePolygons()` misreads high-ring-count polygons on export (`nymcea`, `nypuma2010/2020`); `nynta2020` is a separate, unexplained clip-fragmentation gap | Open | 26b |
| [CSCL-DISTRICTS-02](#cscl-districts-02) | District gdb | Sub-0.5% area deltas on unclipped layers | Open | 26b |
| [CSCL-LDF-01](#cscl-ldf-01) | LDF | Transitory elimination leaves ~3% residual | Open | 26b |
| [CSCL-LDF-02](#cscl-ldf-02) | LDF | `L` and `R` journal record types never published | Open | 26b |
| [CSCL-LDF-03](#cscl-ldf-03) | LDF | Cumulative record number is transcribed, not chained | Open | 26b |
| [CSCL-LDF-04](#cscl-ldf-04) | LDF | One LION record carries `-1` as GENERICID | Open | 26b |

---

## LION

### CSCL-LION-01

**Doubly-reversed proto segments** · Accepted · Last verified 26b

| lionkey | segmentid |
|-|-|
| 3966000330 | 0016558 |
| 2866000025 | 0343093 |
| 2866000020 | 0343094 |
| 2865900235 | 0343095 |
| 2865900230 | 0343096 |

These segments are reversed protosegments. In prod they are not reversed, because of a bug
in the prod ETL:

- geometry-modeled segments are processed one at a time
- during this, protosegments for a given geometry-modeled segment are looked up and processed
- while a protosegment is processed, it refers back to the fields of the geometry-modeled segment
- if a protosegment is reversed, it flips many fields in its representation of the source segment
- if there are multiple reversed protosegments for a single geometry-modeled segment, the
  same fields get flipped back and forth erroneously

**Settled:** GR confirmed this is a prod bug and we will not try to recreate it.

### CSCL-LION-02

**10 rows missing zip code 10035 in production** · Accepted · Last verified 26b

These segmentids are in the middle of Randall's Island, which is all one zip code. All ten
are missing zip in prod: 0246013, 0246014, 0246016, 0246017, 0246018, 0246019, 0246021,
0246022, 0279055, 0279056.

**Settled:** GR has said this is fine.

### CSCL-LION-03

**Curve flag `I` where prod is blank** · Accepted · Last verified 26b

Many rows have `curve_flag` = `I` while prod has blank. These are compoundcurves, which our
pipeline coerces to multistring for geometric operations while prod handles them some other
way; prod's test for whether a curve is "irregular" doesn't work on them.

**Settled:** GR confirmed this is fine, specifically for diffs where
`field = 'curve_flag' AND dev = 'I' AND prod = ' ' AND source_table <> 'centerline'`.

### CSCL-LION-04

**BOE LGC pointer wrong for 568 records in prod** · Accepted · Last verified 26b

Wrong for 568 records. The error is at the face code level and applies to 5 face codes.

**Settled:** GR confirmed ours is right.

### CSCL-LION-05

**Nonstreet feature segment sequence numbers** · Accepted · Last verified 26b

These don't match prod, because they're generated on the fly. They're only generated for
nonstreet feature segments and only used as a unique key within LION, so it doesn't matter
that they differ as long as they're unique.

**Settled:** GR confirmed this is fine.

### CSCL-LION-06

**Coincident segments** · Accepted (root-caused and fixed) · Last verified 26b

segmentid 8101066 (Brooklyn) had a centerline plus *two* `rail_and_subway` segments
(itself plus 8105519, a Subway/Rail crossing at Broadway Junction) all coincident at the
same point - see
[docs/prod_bugs/007-sept-2026-remaining-diffs-investigation.md](./docs/prod_bugs/007-sept-2026-remaining-diffs-investigation.md#1-coincident_seg_count-lion_dat_brooklyn-304118101066)
for the original diagnosis. Root cause: `int__noncenterline_coincident_segments.sql` scoped
its "same type" match to `feature_type`, which merges Subway and Rail into one
`rail_and_subway` value - legacy scopes this to the literal ArcGIS feature class, where
Subway and Rail are separate, so it never counts a Subway/Rail crossing as "same type."
Fixed by matching on `source_table` instead (the actual feature-class distinction),
verified against 8101066 (3 → 2, matching legacy) and confirmed it's the only segment
citywide the change affects.

Related, not yet verified: legacy also gates its centerline-overlap check on the feature's
own `ROW_TYPE != "1"`; ours gates on `segmentid NOT IN int__underground_rail` - a different
condition that may diverge elsewhere. Not touched by this fix.

### CSCL-LION-07

**Center of curvature** · Watch · Last verified 26a

All of these were resolved for 25d by linearizing geoms with a very small tolerance. At
least one returned in 26a.

Related: the working theory for [CSCL-DISTRICTS-02](#cscl-districts-02) is the same
`linearize()` mechanism, so the two may resolve together.

**What would settle it:** a decision with GR — play whack-a-mole per release, or agree the
difference is small enough to accept.

### CSCL-LION-08

**`VIntersect` hardcoded null in `gdb_node`** · Accepted (fixed and verified) · Last verified 26b

Was hardcoded `NULL::text` - the ETL spec has no coverage of this field, and neither
`stg__nodes` nor anything else in our pipeline had a source for it. Resolved (2026-09-16)
from the actual legacy source code (`Create_Node_Shape(12B).py`, J. Ding/ITD-DCP, obtained
directly - not reverse-engineered): a node is `'VirtualIntersection'` iff it's listed in the
`VIRTUALINTERSECTION` table *and* has at least one segment in `STREETSHAVEINTERSECTIONS`;
if it's in `VIRTUALINTERSECTION` with zero such segments, the node is **deleted from the
output entirely**, not just left blank.

Both source tables are already ingested from the same `ETL Working GDB.gdb.zip` every other
CSCL layer comes from (added as `dcp_cscl_streetshaveintersections`/
`dcp_cscl_virtualintersection` in `recipe.yml`), but neither has a `26b` archive yet - only
`26a` (from before these were removed from `recipe.yml`, apparently). Proceeding with the
`26a` data via `missing_versions_strategy: find_latest` per explicit decision.
`compare_gdb.py`'s `KNOWN_STRUCTURAL_DIFFS` entry for `node` was removed since this is a
real computation now, not a placeholder.

**Verified (build 35140557487, 2026-09-16):** `node` layer row counts match exactly
(139,674 = 139,674) and `VIntersect` itself matches exactly on both null rate (97.3% both
sides) and distinct-value count (1 = 1, i.e. only `'VirtualIntersection'` on each side) -
zero flagged columns on the layer. The 26a/26b version-mismatch risk noted above didn't
materialize into any visible diff. Re-check once 26c versions of the two source tables land,
per the standing note that a full re-ingest is imminent.

### CSCL-LION-09

**`node_stname` abbreviation (fixed) / `gdb_altnames` `Join_ID` gap (open)** · Open · Last verified 26b

**`node_stname` - root-caused and fixed (2026-09-16).** The "abbreviation mismatch" framing
below was wrong: there is no abbreviation step in prod at all. JD's legacy source
(`Report_NY_Nodestr(12B).py`, ITD-DCP) revealed the real algorithm - it builds each street
name directly from `dcp_cscl_streetname`'s structured pre/post fields
(`pre_modifier`/`pre_directional`/`pre_type`, `post_type`/`post_directional`/`post_modifier`),
which store the *already-abbreviated* short form (`pre_directional='W'`, `post_type='ST'`).
The old model instead abbreviated `lookup_key`, which stores the spelled-out form (`'WEST 174
STREET'`) - so it was reverse-engineering an abbreviation table to patch over having started
from the wrong column. `gdb_node_stname.sql` was fully rewritten to match the legacy
algorithm: direct field concatenation (asymmetric order - pre puts directional before type,
post puts type before directional, confirmed from source), `dcp_cscl_featurename` overriding
`dcp_cscl_streetname` when both exist for a segment's preferred B7SC, and a segmentid-keyed
fallback (Subway/Rail/Shoreline/NonStreetFeature/Centerline) for segments with neither. Node
membership now comes from `STREETSHAVEINTERSECTIONS` (also just wired up for `VIntersect`,
see CSCL-LION-08) rather than spatial from/to-node adjacency. The old
`node_stname_lastword_overrides` seed and its `dcp_cscl_lastword`/`dcp_cscl_universalword`
abbreviation CTEs are removed - not needed by the real algorithm.

**Verified (build 35144154727, 2026-09-16):** row counts now match almost exactly
(245,444 dev vs 245,482 prod) and only **38 rows differ, all prod-only, zero dev-only** -
down from 31,765 (15,888 dev-only/15,877 prod-only) under the old abbreviation-table
approach. Not yet investigated further; the remaining 38 are a good candidate for the next
round.

**`gdb_altnames`'s `Join_ID` gap** is a separate, larger-magnitude issue with a different
root cause (not an abbreviation problem - see below), still open.

**`gdb_altnames`'s `Join_ID` gap is bigger than the documented SAF-replicant scope can
explain.** `gdb_altnames.sql`'s comment attributes dev's ~50%-of-prod `Join_ID` coverage
(16,617 vs prod's 32,867, this build) to SAF-replicant Join_IDs "not yet produced." Ruled
out so far, across two rounds of investigation:

1. SAF-replicant scope: only 14,759 of 219,931 `include_in_bytes_lion` segments (6.7%)
   carry a `special_address_flag` at all - nowhere near enough to explain a ~2x gap alone.
2. `gdb_altnames.sql`'s `INNER JOIN` to the name-lookup tables: the pre-join B7SC count
   from its `segments`/`b7scs` CTEs alone is already exactly 16,617, identical to the
   final output, so nothing is being dropped there.
3. Blank-vs-null representation (the fix that cut `gdb_lion`'s flagged columns from 75 to
   24 - see `compare_gdb.py`): re-measured dev's/prod's unique `Join_ID` counts after that
   fix landed and they're unchanged, so this isn't a comparison-tool artifact.
4. `gdb_lion.sql` publishes its own `Join_ID` (same macro, same inputs) - it shows the
   identical gap (dev 16,617 vs prod 32,897), confirming the shortfall is upstream of
   `gdb_altnames.sql` entirely, in `int__lion`/`int__streetcode_and_facecode`'s `face_code`
   itself, not anything altnames-specific.
5. `face_code` join failure: `log__lion_segments_missing_facecode` shows exactly 1
   segment citywide with no facecode - the join essentially never fails.
6. Stale/partial source ingest: `dcp_cscl_streetname` (like `dcp_cscl_segment_lgc`,
   already checked in [Bug 007](./docs/prod_bugs/007-sept-2026-remaining-diffs-investigation.md))
   comes from the single-archive `ETL Working GDB.gdb.zip`, one atomic ingest - not a
   version-skew issue.

What's left: `dcp_cscl_streetname` itself has only 6,927 distinct `facecode` values across
73,396 rows (one `principal_flag='Y'` row per facecode, correctly). That's the real ceiling
on `Join_ID` diversity, and it's roughly half of what prod's `Join_ID` count implies it
should be. Two possibilities, both requiring more than code archaeology to resolve: either
this CSCL source snapshot genuinely has fewer distinct facecodes than whatever prod's
legacy pipeline read (a real, structural source-data difference, not a bug), or prod
computes `Join_ID`/`FaceCode` from a different mechanism entirely - e.g. per-segment rather
than per-principal-streetname - that we haven't replicated.

**What would settle it:** GR/legacy-pipeline owner confirming what `Join_ID` is actually
keyed on in the legacy ETL, and whether `dcp_cscl_streetname`'s facecode cardinality has
always been this low or is specific to this source snapshot.

**Separate fix, within the covered `Join_ID`s (2026-09-16): `SName` wasn't truncated to its
30-byte field width.** The coverage-gap analysis above only asks whether a `Join_ID` exists
on both sides. Restricting to the 16,617 `Join_ID`s dev *does* produce and diffing dev's rows
against `production_outputs.fgdb_altnames` (loaded via
`poc_validation.prod_data_loader.load_production_lion_fgdb_layers`) row-for-row still showed
5,723 dev-only / 5,931 prod-only rows even within that shared set. Per ETL spec §2.7.4,
`SName` is a fixed 30-byte field - "truncat[ed] on the right if necessary" - but
`gdb_altnames.sql` emitted the full, untruncated `feature_name`/concatenated street name
(dev's longest ran to 38 bytes). Truncating dev's `SName` to 30 bytes before diffing dropped
the mismatch to 654 dev-only / 877 prod-only (~89% reduction) - confirming this explained
nearly all of it. Fixed with `left(names.sname, 30)`. The residual ~650 rows look like
genuine content differences (e.g. dev `TOMAS MANTON` vs a fuller prod form), not another
representation bug - not investigated further here.

### CSCL-LION-10

**`LegacyID` real-value mismatches on ~2% of segments** · Accepted (comparison-tool artifact, not a data bug) · Last verified 26b

`gdb_lion.sql` computed `LegacyID` as `lpad(legacy_segmentid::text, 7, '0')`, which produces
SQL NULL whenever `legacy_segmentid` is null. Prod's convention for "no legacy ID" is the
literal string `'0000000'`, not blank - so this alone accounted for the vast majority of a
large `LegacyID` null-rate gap flagged in `gdb_lion`'s per-column report. Fixed by
`coalesce`-ing to `0` before padding.

After that fix, on the ~161,000 segments where we have a real (non-`0000000`) `LegacyID`,
160,889 match prod exactly and 156 are null in dev where prod has a real value - both small.
That left **3,170 segments (~2%) with a real `LegacyID` on both sides that simply disagreed**
(e.g. dev `0039154` vs prod `0061538` for the same `LBoro/FaceCode/SeqNum`).

**Root-caused (2026-09-16): the 3,170 figure is a row-alignment artifact of
`compare_gdb.py`'s key choice for the `lion` layer, not a real content mismatch.**
`lion_outputs.csv` keys the `lion` layer on `LBoro|FaceCode|SeqNum`, not `SegmentID` - even
though `gdb_lion.sql` publishes a real, 1:1 `SegmentID` (`lpad(segmentid::text, 7, '0')`).
Rejoining dev's source `legacy_segmentid` straight to prod's `citywide_lion_dat` (cached in
`production_outputs`, loaded by `poc_validation/prod_data_loader.py`) **by `SegmentID`
instead** - across all 218,384 dev segments citywide - gives **zero** real disagreements.
`LBoro/FaceCode/SeqNum` isn't unique or stable enough to be a safe row key here: the lion
layer already has a large row-count gap (219,931 dev vs 243,237 prod, a separate, known scope
gap) large enough that unrelated dev/prod segments coincidentally sharing the same
`LBoro/FaceCode/SeqNum` triple get paired up and reported as content mismatches. (Aside,
unrelated to this fix: prod's own `citywide_lion_dat` has ~1,521 duplicate `segmentid` values
across its 214,466 rows - not investigated further here.)

**Not changed as part of this fix:** `lion_outputs.csv`'s `key_columns` for `lion` is still
`LBoro|FaceCode|SeqNum`. Switching the whole layer's comparison key to `SegmentID` would
likely clean up other reported `lion`-layer diffs the same way, but changes what every
column's dev/prod row-alignment means for that layer, not just `LegacyID` - worth doing as
its own change, tested against the full layer, rather than folded into this note.

**What would settle it:** deciding whether to switch `lion_outputs.csv`'s `key_columns` for
`lion` to `SegmentID` citywide-wide, now that it's confirmed to be a reliable 1:1 key on both
sides.

### CSCL-LION-11

**`segment_locational_status` uses 2010, not 2020, census tracts** · Accepted · Last verified 26b

Legacy incorrectly uses 2010 census tracts (not 2020) when computing `segment_locational_status`'s
`'X'` case (different atomic polygons, same borough, different census tract). We deliberately
match this - `int__segment_locational_status.sql` already carries the note ("TODO all these
2010 fields should be 2020, but this aligns with current ETL tool"). Per
[#2193](https://github.com/NYCPlanning/data-engineering/issues/2193), this is a known legacy
bug, not a spec requirement - flagging here so it isn't "fixed" later and silently
reintroduces a diff against prod.

**What would settle it:** nothing needed to settle the *cause* - this is understood. Only
open question is whether/when GR wants the legacy pipeline itself corrected to 2020, at
which point we'd follow.

### CSCL-LION-12

**Two GR/GSS-flagged discrepancies, not yet resolved** · Open · Last verified 26b

From [#2184](https://github.com/NYCPlanning/data-engineering/issues/2184)'s discrepancy
table - a third item there (segmentid 174704, `special_address_flag`) is already covered by
[Bug 008](./docs/prod_bugs/008-saf-flag-leaks-onto-ramp-protosegment.md); these two aren't
tracked elsewhere:

- **segmentid 133963 (boro 3, face_code 1248, seqnum 01205), `traffic_direction`: dev `A`,
  prod `W`.** Same segment as the RPL node-swap fix in
  [Bug 007 item 4](./docs/prod_bugs/007-sept-2026-remaining-diffs-investigation.md#4-rpl-node-swap-0167138_0133963)
  (a reversed protosegment) - different field/output though, not resolved by that fix. Per
  the issue's own investigation: "the only reversed protosegment where altsegdata_type is
  null, and the only one where this field doesn't match prod."
- **segmentid 241972 (boro 2, face_code 3659, seqnum 00035, shoreline),
  `special_address_flag`: dev `N`, prod blank.** A `commonplace` record with flag `N` exists
  for this segment; prod's corresponding node/protosegment carries no flag at all. Per the
  issue's own investigation, this looks like a genuine prod gap, not ours.

**What would settle it:** both were already flagged to GR/GSS in the original investigation
- no code action pending, just needs their response.

## District gdb

### CSCL-DISTRICTS-01

**Shoreline-clip part counts differ, direction varies by layer** · Open · Last verified 26b

`nymcea` fragments to 249 parts (prod: 122) — prod is singlepart: clipping splits some MCEAs
into disjoint pieces and prod writes each as its own feature, so 115 dissolved (borough, MCEA)
groups become 122 features. Our output splits the same groups into **249** parts.

These aren't slivers — all residual parts are ≥100 sq ft, and the sub-100-sqft filter in
`clipped_geom` was tuned to reproduce prod's part counts on `nycb2010/2020`, `nyct2010/2020`
and `nyed` (still exact as of this check - part counts within a handful on both sides). Total
area matches prod (+0.000%) and attributes are correct.

Re-measured this cycle, the same over-fragmentation-with-matching-area signature also shows on
**`nypuma2010`** (355 dev parts vs 178 prod, every one of the 55 PUMAs affected, area deltas
all <0.002%) and **`nypuma2020`** (210 vs 182) - both dissolve from census tracts and
shoreline-clip the same way `nymcea` does, so this is likely one mechanism, not three. `nynta2020`
also mismatches, but in the **opposite direction** (381 dev parts vs 858 prod - prod is the more
fragmented one here), which doesn't fit a single "we over-fragment" story and needs its own look.

We deliberately did **not** tune a per-layer threshold to force exact part-count matches; that
would be fitting noise rather than understanding the mechanism.

**Root cause found for the over-fragmentation direction (2026-09-16), and it isn't the SQL
clip at all.** Traced `nypuma2010`'s worst case (PUMA 4105: dev 87 parts vs prod's 1) past
the dbt layer entirely:

- Recomputing `clipped_geom`'s exact SQL live gives only **2** real parts (areas
  550,137,046 and 623 sq ft - matching prod's shape almost exactly, 0.0000187% area diff).
  The dissolve-before-clip step is clean too (max 3 parts across all 55 PUMAs, before any
  clipping). So the *data* going into the export is correct.
- The big part alone has **4,927 rings** (`ST_DumpRings`) - one exterior ring plus ~4,926
  small interior holes, from differencing against the water mask's many small subdivided
  pieces along a long coastline.
- `ogrinfo` on our *actual exported* gdb throws `Warning: organizePolygons() received a
  polygon with more than 100 parts` for this feature; the same command against **prod's**
  export of the same layer throws nothing - prod's `Feature Count: 1` reads back clean.
  GDAL's `organizePolygons()` only runs when the ring→polygon/hole nesting is ambiguous on
  read, meaning our FileGDB write doesn't preserve that structure as unambiguously as
  whatever wrote prod's.
- The other 85 "parts" `compare_gdb.py` and QGIS see are `organizePolygons()` misreading
  some of those ~4,926 holes as separate exterior parts (each with a near-zero area, `1e-5`
  sq ft range) instead of subtracting them - not a real geometry difference.

Export goes through `pyogrio.write_dataframe()` in `dcpy/utils/datastores.py`'s
`write_gdb_zip` - **shared infrastructure, not CSCL-specific SQL**. This likely also
explains `nymcea`'s fragmentation (same shoreline-clip-driven high ring count).

**`nynta2020`'s reverse-direction case checked (2026-09-16) - it's not the same mechanism,
and not an export artifact.** Recomputed `clipped_geom`'s exact SQL live for all 262 NTAs:
`ST_NumGeometries` sums to exactly **381** real disjoint parts total, matching our reported
dev part count exactly - so unlike `nypuma`/`nymcea`, our export isn't misreading anything;
381 is genuinely how many parts our clip produces. Prod's 858 is more than double that, on
objectively small polygons (NTAs, not long coastline PUMAs/MCEAs) unlikely to hit the
high-ring-count `organizePolygons()` trigger at all. Whatever's driving prod to fragment
NTAs into more than twice as many pieces as our clip finds - a different/more granular water
mask, a different clip tolerance, no min-area floor on prod's side - is a genuinely separate,
still-open question from the `organizePolygons()` mechanism above.

**What would settle it:** this needs a decision before touching shared code - candidates are
simplifying/consolidating high-ring-count geometry before export, a GDAL layer-creation
option to control `organizePolygons()` behavior on read (`METHOD=SKIP`/`ONLY_CCW`), checking
whether ring winding order (`ST_ForceRHR` or similar) differs from what OpenFileGDB expects,
or a `pyogrio`/GDAL version difference from whatever produced prod's export. All of these
touch code other products' gdb exports depend on.

### CSCL-DISTRICTS-02

**Sub-0.5% area deltas, two different mechanisms** · Open · Last verified 26b

`nyhez` −0.454%, `nycdwi` −0.397%, `nypp` +0.341%. `nyhez`/`nycdwi` are genuinely unclipped
passthroughs (`gdb_nyhez.sql`/`gdb_nycdwi.sql` select straight off their staging models); `nypp`
is not - `gdb_nypp.sql` shoreline-clips via `clipped_geom`/`clip_to_shoreline` like the
`CSCL-DISTRICTS-01` layers, so it needs its own explanation, not this one.

**`nyhez`/`nycdwi` root-caused (2026-09-16): invalid source geometry, resolved differently by
`ST_MakeValid` than whatever prod's pipeline does - not `linearize()`.** Measured area at each
step of `stg__hurricaneevacuationzone.sql`/`stg__communitydistrict.sql`'s
`st_makevalid(linearize(geom))`:

| Layer | Raw area | After `linearize()` | After `st_makevalid()` | linearize Δ | makevalid Δ |
|---|---|---|---|---|---|
| `nyhez` | 13,114,487,444 | 13,114,487,819 | 13,054,906,370 | +0.0000029% | **−0.4543%** |
| `nycdwi` | 13,106,010,319 | 13,106,010,248 | 13,053,921,284 | −0.0000005% | **−0.3974%** |

`linearize()` only affects `ST_MultiCurve`/`ST_MultiSurface` geometry, and even then its effect
here is negligible (sub-0.0001%) - the working theory (same mechanism as `ArcCenterX`/
`ArcCenterY`, [CSCL-LION-07](#cscl-lion-07)) was wrong. The real driver is `st_makevalid()`:
6 of `nyhez`'s 9 source polygons are genuinely invalid before any of our processing -
`ST_IsValidReason` reports "Ring Self-intersection" (3 polygons) and "Nested shells" (3
polygons, one of them alone shrinking 891M→851M sq ft, a ~4.5% drop). This is bad source
geometry, not something our pipeline introduces - PostGIS's `ST_MakeValid` has to choose *some*
resolution for a self-intersecting ring or a shell nested inside another shell, and there's no
reason to expect its choice matches whatever ArcGIS-based repair prod's legacy pipeline applies
to the same invalid input. `nycdwi` wasn't individually inspected polygon-by-polygon but shows
the same signature (linearize negligible, makevalid the whole effect) strongly enough to assume
the same cause.

**`nypp`'s +0.341% is unexplained and likely unrelated** - `stg__nypdprecinct.sql` only calls
`linearize()` (no `st_makevalid()`), and all 78 precincts are already valid after linearize
(`ST_IsValid` true on every one), so the invalid-geometry mechanism above doesn't apply. Its
delta is positive (we have *more* area than prod) where `nyhez`/`nycdwi` are negative, and it
goes through the shoreline-clip macros instead - most likely explanation is a real difference
in the clip itself (water mask boundary, not source validity), still unverified.

**What would settle it:** `nyhez`/`nycdwi` - deciding whether `ST_MakeValid`'s resolution is
acceptable as-is (the source data is objectively invalid; matching prod exactly would require
replicating ArcGIS's specific repair algorithm, not obviously worth it) or worth reporting to
GR as a source-quality issue. `nypp` - still needs its own investigation into the shoreline-clip
mechanics, separate from this entry.

## LDF

### CSCL-LDF-01

**Transitory elimination leaves ~3% residual** · Open · Last verified 26b

Everything else about this output reconciles: every record prod publishes is present in
`CENTERLINEHISTORY`, and our node records reproduce prod's exactly. The sole open question
is **which journal rows prod suppresses before publishing**.

A segment created and destroyed between two releases was never visible to LION users, so its
whole lineage is dropped rather than published. The Phase III document calls this
eliminating transitory records but describes it only as pseudocode that does not match
observed behaviour. The real rule lives in `CSCL_Editor.LDFExtractHelper`, part of the CSCL
Maintenance System, whose source we don't have — the ETL source archived in
`edm-private/cscl_etl/prod_etl_code/` contains only the extract tool's user interface.

We approximate it by building a lineage graph per record type and dropping a connected
component when none of its IDs appear in either LION release:

| Edition | Prod records | Matched | Missing | Extra |
|---|---|---|---|---|
| 26a (25D→26A) | 3,120 | 3,052 | 68 | 99 |
| 26b (26A→26B) | 896 | 860 | 36 | 61 |

97.4% recall at 96.1% precision. Publishing the journal with no elimination gives 99.9%
recall but 511 spurious records, so the step is a clear net gain and still wrong on ~3%.

Two findings constrain any attempt to close this:

- **A rule confined to the journal cannot work.** Deciding "created and destroyed within the
  window" from `CENTERLINEHISTORY` alone fails, because a segment can be created, never
  destroyed, and still be absent from LION — the include/exclude flag and roadway
  jurisdiction filters exclude it. LION membership is genuinely required, which is why
  `int__ldf_segments` reads both releases.
- **The residual should not be tuned away.** It's roughly the same shape across both editions
  tested. Fitting a rule to match prod exactly on two samples would encode coincidence, and
  we'd have no way to tell which.

Because `record_count` and the cumulative record numbers derive from the record set, the
header disagrees with prod too — a consequence of this gap, not a separate defect. Until
the suppression rule is settled the LDF is validating, not releasable.

**What would settle it:** the source or a prose description of `CSCL_Editor.LDFExtractHelper`'s
elimination rule from GR.

### CSCL-LDF-02

**`L` and `R` journal record types never published** · Open · Last verified 26b

Besides `S`, `P` and `G`, `CENTERLINEHISTORY` carries record types `L` and `R` — 19,521 rows
each across all history. Neither appears in any published LDF edition, and per release their
action code counts mirror `G` and `P` exactly, which suggests parallel bookkeeping rather
than emittable records. We drop them on that basis.

**Risk if the assumption is wrong:** every edition we produce is missing two record types.

**What would settle it:** confirmation from GR.

### CSCL-LDF-03

**Cumulative record number is transcribed, not chained** · Open · Last verified 26b

LDF record numbers run consecutively across editions forever. 26a into 26b chains exactly
(565223 + 3611 = 568834). The 25B→25C edition does not chain into 26a: 561831 + 4181 =
566012, while 26a begins at 565223.

GR's tool takes this number as operator input, so the published sequence has at least one
hand-entry gap. We derive it from the previous edition's header instead, which means our
numbers will diverge from prod's if prod's drift again.

**What would settle it:** telling GR about the 25C gap — they may not know, and it's their
sequence.

### CSCL-LDF-04

**One LION record carries `-1` as GENERICID** · Open · Last verified 26b

It reaches the fixed-width output as `00000-1`, which is not a valid zero-filled ID and
breaks a naive integer cast. Exactly one record citywide in 26b. `int__ldf_segments` guards
its casts against it so it doesn't break the build, but this is bad source data rather than
something we should tolerate silently.

**What would settle it:** reporting it to GR.
