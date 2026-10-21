# CSCL Data Issues

Known differences between our output and production, and source-data problems, each with a
stable ID. Code comments, dbt descriptions and the `qa__diffs_*` models cite these IDs where
a diff is accounted for, so an ID is never reused or renumbered.

This file is an index. Each issue's analysis, evidence and history lives in the
data-engineering tracker (beads, prefix `inv`), in the bead listed below. Some issues are
also tracked in GR's
[discrepancy log](https://nyco365.sharepoint.com/:w:/r/sites/NYCPLANNING/itd/edm/Shared%20Documents/DOCUMENTATION/GRU/CSCL/ETL/DE%20Pipeline%20-%20Project%20Tracking/Data%20Discrepancy%20Tracking/LION%20Flat%20Files%20%E2%80%93%20Data%20DiscrepancyIssue%20Tracking.docx?d=w60907e50f8044bd9bffe2508a299035f&csf=1&web=1&e=aZ59n8).
The spec for each output is [design_doc.md](./design_doc.md).

| Status | Meaning |
|---|---|
| **Open** | Unexplained, or explained but undecided. |
| **Accepted** | Understood and deliberately not changing. Usually GR confirmed. |
| **Watch** | Resolved, can recur. Check each release. |

**Last verified** is the product version the issue was last checked against. The bead is
authoritative where it disagrees with this table.

## Issues

| ID | Output | Issue | Status | Last verified | Bead |
|---|---|---|---|---|---|
| CSCL-LION-01 | LION | Doubly-reversed proto segments | Accepted | 26b | inv-tz2.1 |
| CSCL-LION-02 | LION | 10 rows missing zip code 10035 in prod | Accepted | 26b | inv-tz2.2 |
| CSCL-LION-03 | LION | Curve flag `I` where prod is blank | Accepted | 26b | inv-tz2.3 |
| CSCL-LION-04 | LION | BOE LGC pointer wrong for 568 records in prod | Accepted | 26b | inv-tz2.4 |
| CSCL-LION-05 | LION | Nonstreet feature segment sequence numbers | Accepted | 26b | inv-tz2.5 |
| CSCL-LION-06 | LION | Coincident segments | Accepted | 26b | inv-tz2.6 |
| CSCL-LION-07 | LION | `ArcCenterX`/`ArcCenterY`/`Radius` all implemented and fixed to default `0` (not null) on non-curved segments; `Radius` is measured from the rounded center, as prod does - 9,801 of 9,802 curved segments match | Accepted | 26c | inv-tz2.7 |
| CSCL-LION-08 | LION | `VIntersect` hardcoded null in `gdb_node` | Accepted | 26b | inv-tz2.8 |
| CSCL-LION-09 | LION | `node_stname` abbreviation (fixed) / `gdb_altnames`: a SAF Join_ID lists its host segment's names, not its own B7SC's (fixed, 66,056 → 12 diff rows; the rest are Join_IDs only one side's LION has) | Watch | 26c | inv-tz2.9 |
| CSCL-LION-10 | LION | `LegacyID` ~2% mismatch was a comparison-tool artifact (fixed); `lion` layer's key switched to `SegmentID\|Join_ID`; row-level diffing is now partial-precision (fixed, in `dcpy`); prod's `lion`-layer `Join_ID` is missing spec-mandated padding that prod's own `altnames` layer has (tooling accommodation added, not a code fix) | Accepted | 26c | inv-tz2.10 |
| CSCL-LION-11 | LION | `segment_locational_status` uses 2010, not 2020, census tracts | Accepted | 26b | inv-tz2.11 |
| CSCL-LION-12 | LION | Two GR/GSS-flagged discrepancies (133963 traffic_direction, 241972 SAF) | Open | 26b | inv-tz2.12 |
| CSCL-LION-13 | LION | `gdb_lion`'s `Street` field was hardcoded null | Accepted | 26b | inv-tz2.13 |
| CSCL-LION-14 | LION | `coincident_seg_count` = 0 in prod for one Bronx centerline segment, forbidden by spec | Accepted | 26c | inv-tz2.14 |
| CSCL-LION-15 | LION | Generic-segment house numbers not consistently zeroed in prod (real legacy rule found, GP tool, inconsistently applied) | Open | 26c | inv-tz2.15 |
| CSCL-LION-16 | LION | Left/right AtomicPolygon from the AP topology instead of the 2 ft probe (`lion_lr_from_topology`) | Accepted | 26c | inv-law |
| CSCL-DISTRICTS-01 | District gdb | GDAL `organizePolygons()` misreads high-ring-count polygons on export (`nypuma2010/2020`) - fixed by stripping sub-min_area holes in `clipped_geom`; `nymcea` fragmentation and `nynta2020`'s clip-fragmentation gap are separate, still-open mechanisms | Fixed (partial) | 26c | inv-8ju.1 |
| CSCL-DISTRICTS-02 | District gdb | Sub-0.5% area deltas on unclipped layers | Open | 26b | inv-8ju.2 |
| CSCL-DISTRICTS-03 | District gdb | Coastline-adjacent rows show inflated `SHAPE_Length` - root-caused to AtomicPolygon coverage gaps (hairline seams + genuine voids at jurisdictional edges); fixed for `nynta2010`/`nynta2020`, other `clip_to_shoreline` layers still open | Fixed (partial) | 26c | inv-8ju.3 |
| CSCL-DISTRICTS-04 | District gdb | `nybid` BIDID 56 published made valid (prod publishes an invalid nested-shell polygon; geometry and `SHAPE_Area` now agree) | Accepted | 26c | inv-3vi.18 |
| CSCL-LDF-01 | LDF | Transitory elimination leaves ~3% residual | Open | 26b | inv-pkr.1 |
| CSCL-LDF-02 | LDF | `L` and `R` journal record types never published | Open | 26b | inv-pkr.2 |
| CSCL-LDF-03 | LDF | Cumulative record number is transcribed, not chained | Open | 26b | inv-pkr.3 |
| CSCL-LDF-04 | LDF | One LION record carries `-1` as GENERICID | Open | 26b | inv-pkr.4 |
| CSCL-SAF-01 | SAF | SAF diffs once filed as stale prod were all ours. Type S: when an address point's street isn't on its segment's Segment_LGC list (e.g. JFK Airport 497540), prod takes LGC1 from the address point's B7SC with LGC2-4 blank (20 rows); LGC2-4 follow Segment_LGC row order, not value order (0289932); an S record reaching the generic file through the RPL takes the generic segment's name (0170731). ABCEGNPX (Bug 017): first RPL row per roadbed, proto segment for RPL rows, no record for an unnamed CommonPlace. All fixed in `int__saf_*`. Open: duplicate `_saf_key` rows the keyed diff can't see (inv-3vi.11.2.2) | Fixed | 26c | inv-ib0.1, inv-3vi.11.1.2, inv-3vi.11.2 |
| CSCL-THINED-01 | ThinED | Redistricted-field mismatch - resolved by GR's corrected 26c source | Watch | 26c | inv-1xu.1 |
| CSCL-THINED-02 | ThinED | `thined.txt` missing prod's 3-line file header | Watch | 26c | inv-1xu.2 |
| CSCL-THINLION-01 | ThinLION | `nyc.thinlion`'s trailing DOS EOF marker read as a spurious data row (fixed) | Accepted | 26c | inv-1xu.3 |
| CSCL-THINFIRE-01 | ThinFire | Borough field picked an arbitrary AtomicPolygon instead of a spatial match - fixed, 12 companies | Accepted | 26c | inv-1xu.4 |

## Older "Bug NNN" references

Some comments and seeds cite `Bug NNN`, from the bug write-ups that preceded this index.

| Bug | Issue | Bead |
|---|---|---|
| 001 | CSCL-LION-04: BOE LGC pointer, two-digit LGC codes (Queens) | inv-tz2.4 |
| 002 | Police precinct/sector assignment near boundaries (centroid method) | inv-8ju.4 |
| 003 | SAF GNX side-AP fields near AtomicPolygon boundaries (point-in-polygon method) | inv-8ju.5 |
| 004 | CSCL-LION-02: Randall's Island segments missing zip 10035 in prod | inv-tz2.2 |
| 005 | CSCL-LION-01: doubly-reversed protosegments | inv-tz2.1 |
| 006 | 14 diffs reviewed by hand and confirmed correct by JR (Aug 2026). The SAF S row (416070284412) is withdrawn: source says LGC 01, as prod has, and its rule is removed | inv-tz2.16, inv-3vi.11.2 |
| 007 | Six diffs: 1 CSCL-LION-06; 2 Bug 008; 3 corrupt source geometry zeroes district fields; 4 RPL node swap (fixed); 5-6 CSCL-SAF-01 | inv-tz2.6, inv-tz2.20, inv-tz2.17, inv-tz2.18, inv-ib0.1 |
| 008 | `special_address_flag` leaks onto an on/off-ramp protosegment in prod | inv-tz2.20 |
| 009 | CSCL-THINED-01: ThinED redistricted fields | inv-1xu.1 |
| 010 | Exception/Enders/SND normalizing tables carry stale prod entries | inv-8ou |
| 011 | CSCL-LION-09: `gdb_altnames` SAF-replicant Join_IDs | inv-tz2.9, inv-9ko |
| 012 | `gdb_lion` SAF-replicant rows | inv-tz2.19 |
| 013 | FileGDB `CreaDate` shows prod layers that are never regenerated | inv-9k8 |
| 014 | CSCL-THINFIRE-01: ThinFire borough | inv-1xu.4 |
| 015 | CSCL-THINLION-01: `nyc.thinlion` trailing EOF marker | inv-1xu.3 |
| 016 | CSCL-LION-14: `coincident_seg_count` = 0 in prod | inv-tz2.14 |
| 017 | CSCL-SAF-01: SAF ABCEGNPX row-count gaps. Filed as a stale prod snapshot, but they were ours (fixed) | inv-ib0.1, inv-3vi.11.2 |
| 018 | CSCL-LION-15: generic-segment house numbers not zeroed | inv-tz2.15 |
