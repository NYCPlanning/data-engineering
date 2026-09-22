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
| [CSCL-LION-09](#cscl-lion-09) | LION | `node_stname` abbreviation (fixed) / `gdb_altnames` `Join_ID` coverage 51%→93%, remaining diff confirmed as prod-side staleness (CommonPlace N/X alias clusters, not fixable on our side) | Watch | 26c |
| [CSCL-LION-10](#cscl-lion-10) | LION | `LegacyID` real-value mismatches on ~2% of segments | Accepted | 26b |
| [CSCL-LION-11](#cscl-lion-11) | LION | `segment_locational_status` uses 2010, not 2020, census tracts | Accepted | 26b |
| [CSCL-LION-12](#cscl-lion-12) | LION | Two GR/GSS-flagged discrepancies (133963 traffic_direction, 241972 SAF) | Open | 26b |
| [CSCL-LION-13](#cscl-lion-13) | LION | `gdb_lion`'s `Street` field was hardcoded null | Accepted | 26b |
| [CSCL-DISTRICTS-01](#cscl-districts-01) | District gdb | GDAL `organizePolygons()` misreads high-ring-count polygons on export (`nypuma2010/2020`) - fixed by stripping sub-min_area holes in `clipped_geom`; `nymcea` fragmentation and `nynta2020`'s clip-fragmentation gap are separate, still-open mechanisms | Fixed (partial) | 26c |
| [CSCL-DISTRICTS-02](#cscl-districts-02) | District gdb | Sub-0.5% area deltas on unclipped layers | Open | 26b |
| [CSCL-DISTRICTS-03](#cscl-districts-03) | District gdb | Coastline-adjacent rows show inflated `SHAPE_Length` - root-caused to AtomicPolygon coverage gaps (hairline seams + genuine voids at jurisdictional edges); fixed for `nynta2010`/`nynta2020`, other `clip_to_shoreline` layers still open | Fixed (partial) | 26c |
| [CSCL-LDF-01](#cscl-ldf-01) | LDF | Transitory elimination leaves ~3% residual | Open | 26b |
| [CSCL-LDF-02](#cscl-ldf-02) | LDF | `L` and `R` journal record types never published | Open | 26b |
| [CSCL-LDF-03](#cscl-ldf-03) | LDF | Cumulative record number is transcribed, not chained | Open | 26b |
| [CSCL-LDF-04](#cscl-ldf-04) | LDF | One LION record carries `-1` as GENERICID | Open | 26b |
| [CSCL-SAF-01](#cscl-saf-01) | SAF | `lgc1`/`lgc2`/`lgc3` mismatches trace to stale LGC assignments in prod, not a stale load | Open | 26c |
| [CSCL-THINED-01](#cscl-thined-01) | ThinED | Redistricted-field mismatch (`docs/prod_bugs/009`) - resolved by GR's corrected 26c source | Watch | 26c |
| [CSCL-THINED-02](#cscl-thined-02) | ThinED | `thined.txt` missing prod's 3-line file header | Watch | 26c |
| [CSCL-THINFIRE-01](#cscl-thinfire-01) | ThinFire | Borough field picked an arbitrary AtomicPolygon instead of a spatial match - fixed, 12 companies | Accepted | 26c |

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

**Leading-space bug found and fixed (2026-09-18).** `gdb_node_stname.sql` prefixed every
`STNAME` with a literal space, based on a comment claiming "prod prefixes every STNAME with a
single leading space" (inferred from reading the legacy `Create_table_from_txtfile` script,
not from comparing real output - this predates even the 2026-09-16 rewrite, carried over
unquestioned). Checked directly against prod's real, freshly-downloaded 26c
`node_stname` layer (`production_outputs.fgdb_node_stname`): **zero of 245,529 rows have a
leading space.** With `key_columns = NODEID|STNAME`, this single leading-space byte made
every row fail to key-match, producing a 200% "discrepant" rate (245,529 dev-only +
245,529 prod-only, 0 modified) - not a real data problem, a self-inflicted formatting bug.
Removed the leading space; **stripping it makes all 245,529 dev rows match a prod row
exactly, both directions.** Whether the space was ever actually correct for an older release,
or the 2026-09-16 verification's `production_outputs` copy was already stale, is unknown -
the 26b copy that would settle it is empty in the preserved `production_outputs_26b` schema
(consistent with the incomplete-FGDB-load pattern noted elsewhere this cycle). Either way, it
is demonstrably wrong for 26c.

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

**Root-caused (2026-09-18) - possibility #2 above was right, but the fix is blocked, not
closed.** GR pointed at the actual legacy ETL C# source this round (`ExtractorClass.cs`'s
`AddAltNames`/`GetSAFBytesJoinID`) rather than us guessing from output samples. Confirmed: prod
really does compute a large share of its `Join_ID`s from a mechanism unrelated to
`dcp_cscl_streetname`'s facecode diversity - SAF-replicant `Join_ID`s, generated from
`CommonPlace` and `AddressPoint` records whose B7SC has no relationship at all to any segment's
own street classification. Item 1 in the earlier "ruled out" list above measured the wrong thing
(raw segment count carrying `special_address_flag`, 6.7%) rather than how many *distinct
`Join_ID`s* those two source types actually contribute - which turns out to be substantial. A
third SAF source type (`ALTSEGMENTDATA`) was deliberately left unimplemented - traced
`SetSAFStreetNameAndCode` and confirmed it doesn't overwrite the segment's own output LGC
fields, so `AddAltNames`'s regular per-segment path likely already covers it.

Implemented and CI-verified (`ar-cscl-26c`, run `35354234855`): `Join_ID` coverage went from
16,617/32,867 (~51%) to 30,820/33,246 (~93%). **But the row-level diff `compare_gdb.py` actually
reports went from 56,017 (43.18%, pre-session baseline) to 66,076 (50.9%) - net worse**, after
also fixing a second bug (SAF names were joining every StreetName/FeatureName variant instead of
the principal-only row the legacy SAF name lookup actually uses - that alone had made it 100,940/
77.8%). Spot-checking the remaining 13,564 SAF-derived dev-only rows found the same staleness
shape as `CSCL-SAF-01`/`Bug 010`, now in `AddressPoint.B7SC_VANITY`/`B7SC_ACTUAL`: one address
point's current vanity B7SC resolves to "East 14 Street," while prod's real row at that exact
computed `Join_ID` shows "Avenue Y" - the underlying record's vanity code appears to have been
reclassified in CSCL since whatever snapshot prod is stuck on, same as everywhere else this
session. Only checked one example, not confirmed at scale. See
[Bug 011](./docs/prod_bugs/011-altnames-saf-replicant-join-ids.md) for full detail and the
recommendation to confirm this systematically before deciding whether to keep the
`commonplace`/`addresspoint` implementation as-is.

**Confirmed at scale (2026-09-19), with new queryable QA models
(`gdb_altnames_by_field`/`qa_int__prod_fgdb_altnames`/`qa__diffs_fgdb_altnames`, full-row-content
hash diff).** Total diff: 66,056 rows, 77-82% of it concentrated in `CommonPlace`-sourced SAF
Join_IDs (types N/X), not `AddressPoint` as the one-example writeup above suspected. For every one
of the 4,064 distinct problem `Join_ID`s, reconstructed the underlying B7SC's street-code+LGC
suffix and searched current `StreetName`/`FeatureName` under all 5 possible borough digits (the
`Join_ID` formula discards the source B7SC's own borough digit) - **zero matched anything in
current source, under any borough.** Concrete example: `Join_ID` `20079701000000N`'s current B7SC
is unambiguously "BAY PLAZA" (matches dev's single row exactly); prod carries 106 unrelated
alternate names under that same `Join_ID` string (every spelling of "Martin Luther King Jr
Avenue"/"Bartow Avenue"). This is the same staleness pattern as `CSCL-SAF-01`/`Bug 010`, now
confirmed as the dominant driver of the whole-project diff (`qa__diffs_all`), not a code bug. Not
fixable on our side. See [Bug 011](./docs/prod_bugs/011-altnames-saf-replicant-join-ids.md)'s
2026-09-19 update for the full methodology.

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

**Verified (build 35177981946, 2026-09-17):** the full `altnames` layer diff dropped from
65,270 rows (6,093 dev-only / 59,177 prod-only) to 55,132 rows (**1,024 dev-only** / 54,108
prod-only) - dev-only fell by ~83%, consistent with the local estimate above.

**`SType`'s null-rate flag (53.9% dev vs 45.1% prod) traces back to this same coverage gap,
not an independent bug.** `gdb_altnames.sql`'s `names` CTE always sets `SType` (and
`PDir`/`PType`/`SDir`) null for `FeatureName`-sourced rows, by construction - only
`StreetName`-sourced rows can have a real `SType`. 40,082 of dev's 75,729 output rows
(53%) have all four of those fields null simultaneously - i.e. `FeatureName` rows make up
just over half of dev's output. If prod's *covered* population (the same
16,617 `Join_ID`s, once the coverage gap is set aside) has a different `FeatureName`-vs-
`StreetName` mix than dev's, the aggregate null rate would differ for that reason alone,
with no bug in the `SType` logic itself. Given `Join_ID` coverage is already known to skew
by facecode (see above), this is the more likely explanation than a new defect - not
investigated further, since a real fix would mean resolving the `Join_ID` gap itself.

**The residual ~1,024 dev-only rows (within shared `Join_ID`s) point at a deeper mechanism
difference, not a small fixable bug.** Sampled several dev-only rows against prod's actual
`fgdb_altnames` for the same `Join_ID` (e.g. `4389401053100` "Congressman T. Manton
Boulevard", `1237501020405` "85 St Transverse"/"Det. Steven McDonald Way"): prod generates
**dozens of name variants per `Join_ID`** - full and abbreviated spellings, alternate word
orders, and a systematic "EB RB"/"WB RB" (east/westbound roadbed) suffixed duplicate of
nearly every variant (80 rows for `1237501020405` alone). Dev's single bare name for these
`Join_ID`s (e.g. plain `MANTON`, plain `85 ST`) doesn't match any prod variant exactly. This
isn't the `SName`-truncation bug just fixed above - it's evidence for the `Join_ID`
generation-mechanism hypothesis already on record (prod likely builds AltNames per-segment
or per-LGC-variant rather than per-principal-name), not something to chase further without
GR/legacy-pipeline input on how that variant expansion actually works.

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

### CSCL-LION-13

**`gdb_lion`'s `Street` field was hardcoded null** · Accepted (fixed and verified) · Last verified 26b

`Street` (ETL spec §2.7.3/BL1) had never been wired up - `gdb_lion.sql` emitted a literal
`NULL::text` with a "not in int__lion - follow-up work" comment. Fixed (2026-09-16) by adding
a `principal_street_name` CTE to `int__lion.sql`: each segment's preferred B7SC (`int__lgc`,
`lgc_rank = 1`) resolved to its principal name's `LOOKUP_KEY` from `dcp_cscl_streetname`,
overridden by `dcp_cscl_featurename` when both exist for that B7SC - the same source and
override rule AltNames' own `Street` column already uses (spec §2.7.4), just resolved per
segment instead of per `Join_ID`. 100% of `gdb_lion`'s 219,931 rows now populate `Street`;
spot-checked values look correct (unabbreviated full names, e.g. `EAST 34 STREET`,
`UNION SQUARE WEST`).

**Not fixed:** `SAFStreetName` stays `NULL` - per spec §2.7.3 it's populated on SAF replicant
records (copied from `Street`, or from the SAF entry's own principal name for some SAF
types), and SAF replication itself isn't produced (same scope gap as `gdb_altnames`'
Join_ID coverage, CSCL-LION-09) - implementing it here without the replicant records
themselves wouldn't mean anything.

**Verified (build 35177981946, 2026-09-17):** `Street` no longer appears in `lion`'s flagged
columns at all. Null rate matches exactly (0.0% both sides, was 100.0% dev / ~0% prod before
the fix) and distinct-value counts are close (11,159 dev vs 11,190 prod - the small residual
gap is consistent with the existing `Join_ID`/facecode coverage gap, CSCL-LION-09, not a new
problem). `SAFStreetName` correctly still shows up flagged as `KNOWN: unimplemented`.

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

**Fixed (2026-09-19) - root cause was our own SQL, not shared export code.** Ruled out the
"needs a decision before touching shared code" candidates empirically rather than guessing:

- **Ring winding order is already correct.** Checked PUMA 4105's actual geometry: exterior
  ring is clockwise, all 4,926 interior rings are counter-clockwise, 100% consistent - the
  standard, unambiguous convention. Not the cause.
- **`OGR_ORGANIZE_POLYGONS` config (`ONLY_CCW`/`SKIP`) doesn't fully fix it either way, and
  only matters on read, never on write.** Verified: writing with any config setting produces
  byte-identical read-back results to writing with defaults (confirmed via
  `pyogrio.set_gdal_config_options`, `use_arrow=False`, and env vars) - whatever's ambiguous
  about the file is baked in by the OpenFileGDB writer itself, not fixable from the read side.
  `METHOD=ONLY_CCW` on read improves PUMA 4105 from 87 misread parts to 58 (better, still
  wrong); `METHOD=SKIP` makes it worse (4,928 - every single ring treated as its own polygon).
  Would require *every* downstream consumer of our exports to also set this, which isn't
  practical anyway.
- **The real fix: the holes shouldn't exist in the first place.** Checked the size of all
  4,926 "holes" in PUMA 2010's worst case (PUMA 4105): every single one is under 0.24 sq ft
  (max 0.237, min ~1.7e-9) - the same hairline-sliver artifact `clipped_geom`'s existing
  `min_area` floor already removes for whole *parts* (district/water-mask boundaries are
  nominally coincident but differ in the last bits), just never applied to *interior rings
  within* a part that survives that filter. Stripping sub-`min_area` holes the same way:
  PUMA 4105 goes from 4,926 holes to **0**, area changes by 6.9 sq ft (0.0000013%). Below
  GDAL's 100-ring `organizePolygons()` trigger entirely, so the ambiguous-reconstruction
  problem never arises on export.

Implemented in `macros/clip_to_shoreline.sql`'s `clipped_geom` macro (CSCL-local code, not
shared `dcpy` export infrastructure - no cross-product risk). Verified on rebuild:
`nypuma2010` total exported parts 355 → **182** (prod: 178); `nypuma2020` 210 → **180**
(prod: 182) - both now within a handful of prod, the same margin the existing whole-part
`min_area` floor already achieves elsewhere. **`nymcea`'s row count is unchanged (249,
unaffected by this fix)** - contradicts the earlier "likely the same mechanism" note two
paragraphs up; its fragmentation is a genuinely different, still-open mechanism (real disjoint
parts from the dissolve/clip step, not a ring-hole export artifact) and needs its own
investigation. `nynta2020`'s reverse-direction gap (above) is also confirmed separate, unaffected
by this fix.

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
GR as a source-quality issue. `nypp` - see `CSCL-DISTRICTS-03` below, which root-causes the
shoreline-clip mechanism `nypp`'s own delta was left pointing at.

### CSCL-DISTRICTS-03

**Coastline-adjacent features: `SHAPE_Length` inflated well past area-preserving tolerance,
`SHAPE_Area` essentially untouched** · Fixed (partial - `nynta2010`/`nynta2020` only) · Last
verified 26c

Every `clip_to_shoreline`/`clipped_geom`-based layer (`nycc`, `nycd`, `nyed`, `nyha`, `nyfb`,
`nyfc`, `nynta2010`, `nynta2020`, `nypp`, and others) shows a small subset of rows - the ones
whose district actually touches a complex stretch of coastline - flagged as "modified" by
`compare_gdb.py`, but *only* on `SHAPE_Length`: area differs by ~1e-6% to ~1e-9% (i.e. not at
all, in any practical sense) while perimeter differs by anywhere from a fraction of a percent
up to **55%** on the worst-affected rows. This is a different mechanism from the hole-sliver
issue fixed in `CSCL-DISTRICTS-01` (which showed up as extra tiny *parts*/holes, not this).

**Root-caused (2026-09-20/21), two distinct mechanisms, both inside `clip_to_shoreline`'s
`ST_Difference` against `int__water_mask`:**

1. **Hairline gaps between adjacent AtomicPolygons.** Confirmed empirically: nearby
   AtomicPolygons meant to share an edge exactly differ by ~0.0001-0.0002 ft after import (500
   sampled pairs, 0 exactly touching). Individually invisible, but wherever a district's own
   source boundary happens to run *parallel* to a chain of these seams for some distance, the
   gaps accumulate into a long, thin, un-clipped "spur" reaching into the water - confirmed on a
   MN31 (`nynta2010`) spur where checking every AtomicPolygon of any `WATER_FLAG` (not just `1`)
   showed ~43% of the sliver's own footprint has literally no polygon covering it at all.
2. **Genuine AtomicPolygon coverage voids at jurisdictional edges**, unrelated to import
   precision. `stg__neighborhood`'s raw boundary sometimes runs out to a legal line - e.g. the
   NY/NJ state line - that has no reason to coincide with any real AtomicPolygon edge, since
   AtomicPolygons model NYC's own physical geography, not legal jurisdiction. Confirmed on SI12:
   its dominant ~4,300 ft spur is **~100% uncovered by any AtomicPolygon of any flag** within 20
   ft, sits on the NY/NJ state line (visually confirmed against Shooters Island in the Kill Van
   Kull), and the nearest *any* other polygon from its midpoint is 113.9 ft away - one single
   massive water polygon whose own edge simply doesn't reach as far as `stg__neighborhood`'s
   does. No buffer/snap distance fixes this class: there's no second polygon nearby to bridge to.

Independently confirmed against the **real legacy ETL** (`cscl_etl_archive`, C# ArcObjects, not
arcpy as previously assumed): `GDBExtractorClass.cs` clips via `APClipByDissolved`, a real
ArcGIS **Clip** tool (intersect-with-boundary) against a `Select`-then-`Dissolve` of AtomicPolygons
(`APMultiPartDissolve`) - i.e. prod dissolves *land* and intersects, we dissolve *water* and
subtract. These are only equivalent where AtomicPolygons fully tile the area; mechanism 2 above is
exactly a place they don't.

**Fixed for `nynta2010`/`nynta2020` (2026-09-21):**

- `int__water_mask.sql`: water AtomicPolygons are now buffered out 0.01 ft, unioned, and buffered
  back in before subdividing (a "morphological closing") - closes mechanism 1's hairline seams.
  0.01 ft, not a larger value: at 0.05 ft the closing merged two water areas across a real (if
  narrow) non-water feature on MN24, eating a genuine sliver of land prod keeps - 0.01 ft is
  still ~50x the measured gap size with much less room to reach a real feature by mistake.
- `gdb_nynta2010.sql`/`gdb_nynta2020.sql`: the neighborhood polygon is now intersected with its
  own assigned borough (shrunk 5 ft) *before* shoreline clipping - closes mechanism 2, since
  anything outside the district's own borough is dropped regardless of AtomicPolygon coverage.
  This also caught a real, previously-unknown error in `stg__neighborhood` itself: QN99's raw
  polygon extended all the way past Staten Island to Perth Amboy, NJ - a ~2.9B sq ft polygon
  standing in for what should be prod's ~308M sq ft scatter of Queens parks/cemeteries. Bounding
  to Queens' own extent brings the area back in line with prod to within 0.24%.

**Results on `nynta2010`** (`SHAPE_Length` gap vs. prod): SI12 14,353 ft → **-9 ft**; QN45 9,614
ft → **-55 ft**; QN98 12,728 ft → 424 ft (better, not fully closed - unexplained residual);
MN31/BK29 → 0 ft exactly. A handful of rows (QN99 -4,021 ft, BK29 1,585 ft, MN34 1,370 ft, and a
few others under ~600 ft) are new/changed smaller-magnitude gaps from the 0.01 ft buffer choice,
not yet individually root-caused - QN99's is very likely just the new boundary tracing along the
borough line itself (its `SHAPE_Area` matches prod to 0.24%, so the underlying shape is right).

**Not yet done:** the other `clip_to_shoreline` layers (`nycc`, `nycd`, `nyed`, `nyha`, `nyfb`,
`nyfc`, `nypp`, etc.) still use the un-bounded water mask only (they get mechanism 1's fix for
free via `int__water_mask`, but not mechanism 2's borough-bound fix, which was only added to the
two NTA models so far). Worth auditing each for whether its own source geometry ever runs past
its borough/jurisdiction the same way `stg__neighborhood` does before deciding whether to extend
the borough-bound pattern to them too.

**Audited 2026-09-21 - `nycc`, `nyfd`, `nypuma2010`, `nypuma2020`.** Compared each layer's
per-row `SHAPE_Length`/`SHAPE_Area` directly against prod's real district gdb (downloaded from
`edm-private/cscl_etl/26c/v26C_Districts.gdb.zip`, matched on each layer's declared key). All
four show exactly this issue's signature - `SHAPE_Area` within noise while `SHAPE_Length` is
inflated on a subset of rows - concentrated on Queens' most complex/marshy coastline:

- `nycc`: 35/51 districts differ at all; worst is CD31 (+5.67%, 20,842 ft) and CD19 (+1.48%),
  both Queens waterfront districts (Rockaway/Jamaica Bay, Whitestone/Little Neck Bay). CD8 - the
  one council district that genuinely spans two boroughs (East Harlem/Manhattan and Mott
  Haven/Bronx, connected across the Harlem River - confirmed real, not a data error) - is *not*
  a source of error: its `SHAPE_Length` matches prod to 0.0002%.
- `nyfd`: 9/9 divisions differ; worst is FireDiv 13 (+2.04%, 13,985 ft) - also the only row with
  an area delta above noise (-0.118%, vs <0.005% on the other 8), suggesting something beyond a
  pure hairline-seam issue. FireDiv 13 is ~99.9% Queens (a sliver of Brooklyn) - same coastline
  profile as `nycc`'s worst offenders.
- `nypuma2010`/`nypuma2020`: every row differs, up to 13.6% (`nypuma2020` PUMA 4403). Worst
  offender both editions is PUMA 4105 (Rockaway peninsula/Jamaica Bay) - the same PUMA already
  flagged as `CSCL-DISTRICTS-01`'s worst ring-count case, so this is evidently a second, distinct
  issue on the same especially complex piece of coastline.

**Ruled out for all four: the `nynta`-style borough-bound fix (mechanism 2).** Checked whether
any of these layers' raw source geometry extends past its own assigned borough the way
`stg__neighborhood`'s did (QN99/Perth Amboy, SI12/NJ state line) - none of the *problem* rows do
(CD31/19/32, FireDiv 13, PUMA 4105 are all >99.9% inside a single borough). So the residual here
isn't a jurisdictional-edge coverage void; these layers already get mechanism 1's hairline-seam
closing for free via the shared `int__water_mask.sql` macro, and it evidently isn't sufficient on
Queens' more convoluted marsh/bay coastline (many more, smaller AtomicPolygon seams per mile of
district boundary than the NTA cases that motivated the 0.01 ft buffer). Extending the
borough-bound pattern to these layers would add code with no expected effect - don't.

**New, unrelated finding while checking `nypuma`'s borough containment:** one of
`dcp_cscl_censustract2010`'s source tracts feeding PUMA 4101's dissolve carries `BoroCode=5`
(Staten Island) and contributes 864M sq ft - roughly a third of the dissolved PUMA's raw area -
to a PUMA that is otherwise 60/62 tracts of Queens (`BoroCode=4`); a second tract is mislabeled
`BoroCode=3` (Brooklyn). Both are almost certainly open-water Census tracts (this stretch is the
Rockaway Inlet/lower Jamaica Bay, between Queens and Staten Island) that get clipped away by
`clipped_geom`'s water subtraction regardless, so they likely don't explain the `SHAPE_Length`
gap above - PUMA 4101 itself isn't among the worst-affected rows (+0.95%). Flagging since it's
the same shape of source-data mislabeling as QN99's Perth Amboy polygon, just apparently
inconsequential here.

**Still not done:** the actual coastline-seam-gap fix for these Queens-heavy layers - likely
needs sampling actual gap sizes along CD31/FireDiv13/PUMA4105's boundaries the way SI12's
~0.0001-0.0002 ft gaps were measured for `CSCL-DISTRICTS-01`, to see whether a larger (but still
safe - see the MN24 regression note above) closing buffer would close them, or whether it's a
structurally different gap (e.g. real multi-hundred-foot voids between Jamaica Bay's marsh
islands) that no single buffer size can close. FireDiv 13's above-noise area delta specifically
deserves its own look before assuming it's the same mechanism as the others.

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

## SAF

### CSCL-SAF-01

**`lgc1`/`lgc2`/`lgc3` mismatches trace to stale LGC assignments in prod, not a stale load** ·
Open · Last verified 26c

`saf_s_generic`/`saf_s_roadbed`'s unaccounted `modified` rows are dominated by one pattern:
prod's `lgc1 = '01'`, dev showing a specific code (`03`/`04`/`05`/`10`/`11`/`14`) that prod
doesn't have - **9 of 11** `saf_s_generic` and **9 of 13** `saf_s_roadbed` unaccounted rows
this build. `docs/prod_bugs/007-sept-2026-remaining-diffs-investigation.md` originally
attributed segmentid 136981's version of this to a stale `production_outputs` load (SAF's
generic `load` command didn't track versions at the time). That theory is retracted: SAF was
reloaded fresh for 26c with proper version tracking, and the identical mismatch persists.
`dcp_cscl_segment_lgc` for segmentid 136981 still has only `03` (preferred) and `02` - no
`01` anywhere - so our output (`lgc1=03, lgc2=02`) is correct given current source; `01`
itself isn't rare or retired (201,372 of 319,257 current `dcp_cscl_segment_lgc` rows carry
it), it's just gone specifically for these segments, replaced by a more specific
classification prod's SAF hasn't picked up.

This is the same shape of problem as `Exception.txt`/`Enders.txt`/`SND.txt`'s stale entries
(`docs/prod_bugs/010-featurename-normalizing-tables-stale-accretion.md`) - prod's derived
output not fully regenerated from current source each cycle - now confirmed in a fourth
output family. Not yet folded into that doc's scope explicitly; tracked separately here since
the source table (`dcp_cscl_segment_lgc`) differs from the FEATURENAME-derived ones.

Two smaller, distinct anomalies on the same two files, not yet root-caused:
- One segment (`27080026604206151R.../268020`) has 3 SAF sub-records whose keys don't align
  between dev and prod (`only_in_legacy`/`only_in_build`, 3 each, identical in both
  `saf_s_generic` and `saf_s_roadbed`) - the key embeds `segment_seqnum`, and dev's values for
  this segment's multiple sub-records look offset from prod's, not missing outright.
- Two rows (`43500017073100790R`, both roadbed sides) have dev's `place_name` blank where
  prod has `NORTH BOUNDARY ROAD`.

`saf_i`, `saf_d_generic`, `saf_d_roadbed`, `saf_ov_generic`, and `saf_ov_roadbed` show zero
diffs against the fresh 26c reload - confirms the reload itself was not the problem for those
files specifically. `saf_abcegnpx_generic`/`saf_abcegnpx_roadbed` retain a handful (3-7) of
row-count-only mismatches, not yet traced but consistent in scale with the already-open,
unimplemented SAF-replicant scope (see `docs/ETL_V8_02012024.md`, "Segment Replication for SAF
Data").

**What would settle it:** ask GR whether `saf_s_*`'s LGC values are regenerated from CSCL's
current `Segment_LGC` table each release or carried forward, using segmentid 136981 as a
concrete example (current source has no LGC `01` for it at all).

## ThinED

### CSCL-THINED-01

**Redistricted-field mismatch (`docs/prod_bugs/009`) - resolved by GR's corrected 26c source** ·
Watch · Last verified 26c

`docs/prod_bugs/009-thined-redistricted-field-mismatch.md` documented real mismatches in the
four fields tied to offices redistricted since the last cycle (`congress_district`,
`state_sen_district`, `muni_court_district`, `city_council_district`), hypothesized as a
source-vintage skew between whatever `ElectionDistrict` snapshot our 26c archive held and
whatever prod's separate legacy pipeline used to generate its own `thined.txt`. GR sent a
corrected `thined.txt` for 26c; comparing our (unchanged) output against it directly
(`assembly_district + election_district` keyed, as bug 009 established) shows **zero** field-level
mismatches across all 4285 rows - full multiset match, confirmed with `sort`/`comm` (the same
method `validate_outputs.sh` uses) as well as a keyed per-row check. The hypothesis in bug 009
is the likely explanation: our source data was fine, prod's `thined.txt` snapshot just wasn't
freshly-vintaged. No code change needed here - see `CSCL-THINED-02` for the one remaining,
purely structural gap this same comparison surfaced.

**What would settle it:** nothing further needed on our side; watch for recurrence next cycle if
GR's `thined.txt` and `ElectionDistrict` snapshots drift out of sync again.

### CSCL-THINED-02

**`thined.txt` missing prod's 3-line file header** · Fixed · Last verified 26c

Once `CSCL-THINED-01`'s real data mismatch was resolved, `validate_outputs.sh` still reported 3
discrepant rows for `thined.txt`. Root cause: prod's real `thined.txt` carries a 3-line,
14-byte-wide file header *before* the data records - our build emitted only the 4285 data rows,
no header. `comm -13` (prod-only) on the sorted raw files showed exactly the 3 header lines;
`comm -23` (dev-only) was empty, consistent with `CSCL-THINED-01`'s finding that the data itself
is a perfect match.

The header isn't documented in `design_doc.md`/`docs/ETL_V8_02012024.md`, and isn't produced by
the legacy ETL tool archived in `cscl_etl_archive` - `etl_docs.MD` states explicitly "The ETL
tool produces two types of district equivalency files: ThinLION and ThinFire," with no ThinED
extractor class anywhere in that archive. `thined.txt`, like the LDF, is evidently built by a
separate legacy tool we don't have source for.

Header record layout (each line 14 bytes, same width as a data record):

| Line | Content | Meaning |
|---|---|---|
| `0000THIN260309` | record type `0000` + `THIN260309` | Unknown exact meaning; the trailing 6 digits parse as a plausible `YYMMDD` (2026-03-09), but nothing in this codebase or the legacy archive confirms that reading or what date it should represent. |
| `000126A1      ` | record type `0001` + `26A1` + 6 spaces | Unknown exact meaning; looks like an edition/version tag, but `26A1` doesn't correspond to this build's own `26c` version - plausibly a stale/frozen tag prod's tool carries forward without updating each cycle, in the same spirit as the frozen 2009-batch district gdb layers (`docs/prod_bugs/013`), though unconfirmed. |
| `000200004288  ` | record type `0002` + record count `00004288` + 2 spaces | **Understood and computed.** Self-referential: counts every line in the file, header included (3 + 4285 = 4288) - the same "record count includes header record" convention `design_doc.md` documents explicitly for the LDF header (`LDFH6`). |

**Fixed for 26c** in `models/product/thined/thined_dat.sql`: prepends a `header` CTE ahead of
the data rows. The record-count line (`0002`) is computed from the real row count, since we
understand its rule precisely. The other two lines' payloads (`thined_file_tag`, `thined_version`)
live in `seeds/config.csv` (see `macros/config_value.sql`) rather than being literals in the SQL -
not derived from any understood formula, since we have no source confirming what (if anything)
should change about them release to release, but at least editable in one obvious place instead
of a SQL string. Verified against 26c's real prod `thined.txt`: zero dev-only, zero prod-only
rows.

**Deliberately not attempted:** reverse-engineering an update rule for `thined_file_tag`/
`thined_version` well enough to compute them for a future release. Given neither this codebase
nor the available legacy source explains their real semantics, guessing a formula risks
encoding coincidence as fact - the same trap `CSCL-LDF-01` warns against for tuning to match a
small sample. The `seeds/config.csv` values will silently go stale (byte-mismatch reappearing as
this same 3 discrepant-row pattern) the moment either value legitimately changes upstream - see
that seed's per-row description for the same caveat, and the "Follow up with GS" note above.

**What would settle it:** ask GR what `thined.txt`'s file header actually encodes, and whether
`26A1`/`THIN260309` are expected to change release to release or are effectively frozen
constants from whatever tool originally generated this format. **Follow up with GS** - since
neither `design_doc.md`/`docs/ETL_V8_02012024.md` nor `cscl_etl_archive` document this format,
Geosupport (the actual consumer of this file) may know what it's for even if GR doesn't.

## ThinFire

### CSCL-THINFIRE-01

**Borough field picked an arbitrary AtomicPolygon instead of a spatial match (fixed)** ·
Accepted · Last verified 26c

12 fire companies (`E068`, `L052`, `E202`, `E205`, `E221`, `E224`, `L106`, `L118`, `E262`,
`L115`, `E266`, `L173`) appeared in the wrong borough's ThinFire file. `thinfire_by_field_
unformatted.sql`'s `TF5`/Borough logic picked whichever AtomicPolygon tagged with a company's
code happened to have the lowest `atomicid` - meaningless for any company whose territory spans
an AtomicPolygon in more than one borough, which is completely normal (rivers, bridges,
boundary streets). 10 of the 12 landed in Manhattan regardless of the company's real borough -
not a Manhattan-specific mechanism, just that Manhattan's AtomicPolygons happen to have lower
IDs, so any company touching it at all got biased there.

`docs/ETL_V8_02012024.md` Table 25 Note 1 explicitly authorizes this - "Populate TF5 with the
BOROUGH attribute of any of the matching AP's" - as an ESRI-desktop-era performance shortcut
for a spatial method (company centroid, point-in-polygon against boroughs) the spec itself
describes as the "proper" approach. Not a deviation from spec; the spec's own arbitrary choice
just doesn't have to agree with ours.

**Fixed:** replaced the AtomicPolygon lookup with the spec's own preferred method - company
centroid (falling back to `ST_PointOnSurface`) point-in-polygon against `stg__borough`, the same
pattern already used for police precinct/sector/patrol-borough assignment
([Bug 002](./docs/prod_bugs/002-police-geo-centroid-mismatch.md)). Verified before
applying: AtomicPolygon count-majority, AtomicPolygon area-majority, and the spatial method all
independently agreed on the correct borough for all 12 known-mismatched companies *and* the 3
already-hardcoded special cases (`E-81`, `E-260`, `E-263`) - the spatial method was chosen for
consistency with the existing police-geography pattern. Full company-roster regression check:
exactly those 12 companies change, zero others. `qa__diffs_thinfire_{bronx,brooklyn,manhattan,
queens,statenisland}` all return 0 rows after the fix. See
[docs/prod_bugs/014-thinfire-borough-arbitrary-atomicpolygon-pick.md](./docs/prod_bugs/014-thinfire-borough-arbitrary-atomicpolygon-pick.md).

**Related, separate finding:** prod's ThinFire delivery is byte-identical to the 26b snapshot
across all 5 boroughs (confirmed via content hash) - GR's legacy pipeline doesn't regenerate
this file family each cycle, the same pattern as `Enders.txt`/`Exception.txt`/`SND.txt`
(`docs/prod_bugs/010`) and the frozen district gdb layers (`docs/prod_bugs/013`). Doesn't affect
this fix (verified against prod's actual, if stale, values) but ThinFire should be re-checked
whenever prod's delivery is eventually refreshed.

**What would settle it:** nothing further needed for the fix itself. Worth reporting the frozen
ThinFire delivery to GR alongside the other stale-regeneration findings.
