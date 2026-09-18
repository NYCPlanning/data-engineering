# District GDB geometry discrepancies: what's actually going on

Explains why several district gdb layers show geometry-related diffs against prod, what
export mechanism we use, and why "the geometries look different" is actually three separate
mechanisms with different implications. Companion to `data_issues.md`'s `CSCL-DISTRICTS-01`
and `CSCL-DISTRICTS-02` entries, which have the full numbers and are the authoritative,
versioned record - this is the narrative version for bringing someone up to speed quickly.

## Scope

Mostly **district gdb** (the polygon layers with shoreline clipping and/or curved source
geometry), but not exclusively - LION gdb has its own, unrelated geometry issue
(`ArcCenterX`/`ArcCenterY`, the curve-center-of-curvature fields, tracked as `CSCL-LION-07`,
currently on hold). Everything below is specific to district gdb's polygon layers.

## Our export mechanism

`dcpy/utils/datastores.py`'s `write_gdb_zip()` calls:

```python
pyogrio.write_dataframe(gdf, gdb_path, driver="OpenFileGDB")
```

**`OpenFileGDB` is GDAL's open-source reimplementation of the FileGDB format** - not ESRI's
own proprietary FileGDB SDK. GDAL actually ships a *separate* driver, `"FileGDB"`, that
wraps ESRI's real SDK; we're not using it. Prod's original FGDB was almost certainly written
by ArcGIS/ArcPy itself (the legacy ETL scripts obtained for this project are `arcpy`-based),
i.e. by ESRI's own writer. This driver mismatch turns out to matter for one of the three
mechanisms below.

This is shared infrastructure - `write_gdb_zip` is used by every product's gdb export, not
just CSCL. Any fix here needs a cross-team decision, not a CSCL-only patch.

## Three separate mechanisms

It's tempting to lump these together as "the geometry import/export pipeline is lossy," but
they're distinct, have different root causes, and (importantly) different implications for
whether the output is "wrong."

### 1. Curve linearization - real, but confirmed negligible

Some source layers arrive from the source FGDB as `ST_MultiSurface`/`ST_MultiCurve` (real
circular arcs) rather than straight-sided polygons - e.g. hurricane evacuation zones.
`stg__hurricaneevacuationzone.sql` and similar staging models call:

```sql
st_makevalid(linearize(geom))
```

`linearize()` (`SELECT prosrc FROM pg_proc WHERE proname='linearize'`) calls
`ST_CurveToLine(geom, 0.00025, 1)` for `MultiCurve`/`MultiSurface` geometry - converting
circular arcs to straight-line segments with a 0.00025 ft deviation tolerance. This
conversion is real and does happen.

**Measured impact:** for `nyhez` (hurricane evacuation zones, `ST_GeometryType` shows 8 of 9
source polygons are `ST_MultiSurface`), area before vs. after `linearize()`:

| | Area (sq ft) |
|---|---|
| Raw | 13,114,487,443.58 |
| After `linearize()` | 13,114,487,818.70 |
| Δ | +0.0000029% |

**Conclusion: negligible.** This does happen, and it's a real, lossy conversion in
principle, but it is not the cause of any diff we've actually found. Don't chase this one
further as an explanation for area/fragmentation mismatches.

### 2. Genuinely invalid source polygons, resolved by `ST_MakeValid` - unrelated to curves

This is the cause of `nyhez`'s -0.454% and `nycdwi`'s -0.397% area deltas
(`CSCL-DISTRICTS-02`). It has nothing to do with curves - it's a polygon-*validity* issue,
independent of whether the source geometry was ever curved.

Checking `ST_IsValid`/`ST_IsValidReason` on `nyhez`'s 9 source polygons (after `linearize()`,
before `ST_MakeValid()`) found **6 of the 9 are genuinely invalid**:

| Reason | Count |
|---|---|
| Ring Self-intersection | 3 |
| Nested shells | 3 |

One "nested shells" case alone shrinks from 891M to 851M sq ft (~4.5%) once made valid.
This is bad source geometry, full stop - present before any of our processing touches it.

`ST_MakeValid()` has to pick *some* resolution when it encounters a self-intersecting ring
or a shell nested inside another shell - there is no single "correct" answer for genuinely
invalid input. There's no reason to expect PostGIS's resolution choice matches whatever
ArcGIS-based repair prod's legacy pipeline applied to the same broken input.

**Verdict: the output geometry here is valid and functional.** It's not "screwed up" - it's
a legitimate, defensible resolution of ambiguous/broken input that happens to differ from
prod's equally-legitimate resolution. Matching prod exactly would mean replicating ArcGIS's
specific repair algorithm, which is a very different bar than "is our geometry correct."

### 3. `organizePolygons()` misreads our export - this is the one that's actually "screwed up"

This is the cause of `nymcea`'s and `nypuma2010`/`nypuma2020`'s part-count fragmentation
(`CSCL-DISTRICTS-01`) - e.g. `nymcea` exports as 249 parts where prod has 122; one PUMA
(4105) showed 87 parts in our export vs. prod's 1.

**The underlying computed geometry is correct.** Recomputing `clipped_geom`'s exact SQL live
for PUMA 4105 gives exactly **2** real parts (555,137,046 and 623 sq ft), matching prod's
shape to within 0.0000187% area. The dissolve-before-clip step is clean too. So *the data
going into the export is right*.

The problem is at **export time**. The big part alone has **4,927 rings**
(`ST_DumpRings`) - one exterior ring plus ~4,926 small interior holes, produced by
differencing the district polygon against a water mask that's itself subdivided into many
small pieces along a long, detailed coastline.

Running `ogrinfo` against **our own exported gdb** for this layer throws:

```
Warning: organizePolygons() received a polygon with more than 100 parts
```

Running the identical command against **prod's** export of the same conceptual layer throws
**nothing** - prod's `Feature Count: 1` reads back clean.

`organizePolygons()` is the GDAL routine that decides, when reading a FileGDB back, which
rings are holes belonging to which exterior shell (FileGDB's on-disk ring encoding doesn't
make this fully explicit in all cases - the reader has to reconstruct shell/hole nesting
from ring geometry). When that reconstruction is ambiguous, `organizePolygons()` guesses,
and here it guesses wrong for ~85 of the ~4,926 holes - misreading them as separate exterior
shells (each a near-zero-area sliver, `1e-5` sq ft range) instead of subtracting them as
holes. That's where the extra 85 "parts" `compare_gdb.py` (and QGIS) see come from - they
aren't real geometry, they're a read-time misassembly artifact.

**Why does prod's export not trigger this?** Almost certainly the driver mismatch noted
above: prod's FGDB was very likely written by ArcGIS/ArcPy (ESRI's own writer), while ours
goes through GDAL's `OpenFileGDB` driver (an independent, open-source implementation of the
same on-disk format). It's plausible - not yet confirmed - that ESRI's writer encodes ring
nesting in a way that's unambiguous on read, while `OpenFileGDB`'s writer doesn't, for
polygons with very high ring counts.

**Verdict: this is the one that's genuinely "screwed up," not just "different."** Our
database geometry is correct, but the *file we hand out* - opened with `ogrinfo`, QGIS, or
presumably ArcGIS itself - shows phantom sliver polygons that don't correspond to anything
real. Anyone consuming the published file sees corrupted-looking output, even though
nothing is actually lost or wrong in the underlying computation.

## What would actually fix #3 (not yet tried)

Given the likely driver mismatch, the most promising untried option is switching
`write_gdb_zip` from `driver="OpenFileGDB"` to GDAL's `"FileGDB"` driver (which wraps ESRI's
own SDK) for this export - or at least testing whether that avoids the `organizePolygons()`
ambiguity on high-ring-count polygons. Other candidates already on record in
`CSCL-DISTRICTS-01`:

- Simplifying/consolidating high-ring-count geometry before export (e.g. `ST_Simplify` or
  merging adjacent small holes) - a lossy workaround, not a real fix.
- A GDAL layer-creation option to control `organizePolygons()` behavior on read
  (`METHOD=SKIP`/`ONLY_CCW`) - would need testing against our actual export.
- Checking whether ring winding order (`ST_ForceRHR` or similar) differs from what
  `OpenFileGDB` expects - **not yet checked**; `_normalize_to_single_geom_type` in
  `datastores.py` (the pre-export normalization step) does *not* currently touch winding
  order at all, only single-vs-multi geometry type promotion.

**None of these have been tried yet.** This needs a decision before anyone touches
`write_gdb_zip` - it's shared code every product's gdb export depends on, not a
CSCL-specific fix.

## The two layers that don't fit either story

- **`nynta2020`** fragments in the *opposite* direction (858 prod parts vs. our 381) and was
  explicitly checked against the `organizePolygons()` theory: recomputing the SQL live gives
  exactly 381 real disjoint parts, matching our reported count exactly - our export isn't
  misreading anything here. Prod is genuinely more fragmented, on objectively small polygons
  (NTAs, not long coastline PUMAs/MCEAs) unlikely to hit the high-ring-count trigger at all.
  Still unexplained - candidates are a different/more granular water mask on prod's side, a
  different clip tolerance, or no minimum-area floor on prod's side.
- **`nypp`** (+0.341% area) was originally miscategorized as an unclipped passthrough
  alongside `nyhez`/`nycdwi` - it's actually shoreline-clipped like the layers in mechanism
  #3. Its source geometry (`dcp_cscl_nypdprecinct`) is 100% valid after `linearize()` (0 of
  78 precincts invalid), so mechanism #2 doesn't apply either. Likely something in the clip
  itself (water mask boundary difference) - unverified.

## Bottom line

A full scan of every district gdb layer in the latest build confirmed **zero layers have a
non-geometry attribute bug** - every diff is one of the three mechanisms above, one of the
two unexplained cases, or a generic sub-mechanism-2/3-sized geometry mismatch too small to
have been worth root-causing individually. We're cleanly blocked on geometry (plus one
unrelated, zero-impact schema quirk in `nyura` - prod's published schema for that layer is a
stale copy-paste from an unrelated layer, `nybid`, and both sides have 0 rows so it's
invisible in practice).
