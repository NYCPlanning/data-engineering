# Bug 013: FileGDB Item Metadata Confirms Systemic "Never Rebuilt" Pattern Across Products

**Status:** Confirmed (corroborating evidence, not independently actionable) - explains an
already-known oddity (`nyura`/`nybid`)
**Affected Output:** `gdb_altnames` (LION gdb); `nyura`, `nybid`, `nycdwi`, `nyhd`, `nymcea`,
`nyzip`, `nyad`, `nybb`, `nyap` (District gdb)
**Severity:** N/A - not a new discrepancy, corroborating evidence for `Bug 011` and the existing
`nyura` structural-diff note in `compare_gdb.py`
**Discrepancy Count:** N/A (metadata-only finding, no row-level count)

## Summary

Every FileGDB carries per-item ESRI metadata (creation date, modification date, processing
lineage) inside its own system catalog. Read directly (via `ogrinfo`/GDAL's `OpenFileGDB`
driver), it tells a clean, independently-verifiable story about which layers get freshly rebuilt
each release cycle and which don't - corroborating this whole session's "prod accumulates data it
never fully regenerates" theme with hard, byte-level evidence rather than just row-content
inference.

## Technical Details

### LION gdb (`nyclion_26b.zip`/`nyclion_26c.zip`, modern "GDB_Items" catalog format)

Queried via `ogrinfo -sql "SELECT Name, Documentation FROM GDB_Items"` (the `Documentation` field
holds embedded FGDC/ESRI metadata XML, including a `CreaDate` tag). All four layers in the LION
gdb (`node`, `node_stname`, `altnames`, `lion`) were assembled into their respective containers on
a single day each cycle (26B: 2026-05-14; 26C: 2026-08-10 - confirmed via the workspace-level
processing lineage, e.g. `CreateFileGDB` -> `FeatureClassToGeodatabase node` -> `TableToGeodatabase
node_stname` -> `TableToGeodatabase altnames`, all within one minute of each other). Their item
`CreaDate`s:

| Layer | 26B `CreaDate` | 26C `CreaDate` |
|---|---|---|
| `node` | 2026-04-29 | 2026-07-28 |
| `node_stname` | 2026-05-04 | 2026-07-29 |
| `lion` | 2026-05-14 | 2026-08-10 |
| `altnames` | **2009-01-05** | **2009-01-05** |

`node`/`node_stname`/`lion` all show a `CreaDate` matching their own release's assembly window -
each is a genuinely different date, incrementing release over release, proving they're really
recreated fresh. `altnames`'s `CreaDate` is **byte-identical across two consecutive releases**,
seventeen years old, sitting next to three siblings that were demonstrably rebuilt the same day.
This is independent, mechanical confirmation of [Bug 011](./011-altnames-saf-replicant-join-ids.md)'s
content-level finding (prod's `altnames` output contains large clusters of alias names with no
current-source correspondence) - the underlying table object itself has not been recreated since
2009, only re-exported.

The embedded FGDC abstract for `altnames` also documents the many-alias-spellings feature as
intentional design (worked example: "Adam Clayton Powell Boulevard" -> "Powell Boulevard"/
"A C Powell Boulevard") - the mechanism is real and deliberate, just not kept in sync with current
source for a large fraction of entries.

### District gdb (`v26B_Districts.gdb`, classic pre-10.x FileGDB format)

An older FileGDB schema (`GDB_SystemCatalog`/`GDB_ObjectClasses`/... rather than the LION gdb's
`GDB_Items`) - metadata lives in a separate `GDB_UserMetadata` table (found by probing
`a0000000*.gdbtable` files directly for one exposing `Name`/`Xml` columns), one row per layer that
has ever had metadata attached (11 of the gdb's 43 layers - most have none at all, stale or
otherwise).

Extracted every `ModDate`/`SyncDate`/`CreaDate`/`procdate` timestamp recorded in each layer's
*entire* metadata history (not just the outermost `CreaDate`), to distinguish "touched once, long
ago, and never again" from "touched periodically but the creation-date field doesn't show it":

| Layer(s) | Every date ever recorded | Verdict |
|---|---|---|
| `nybid`, `nycdwi`, `nyhd`, `nymcea`, `nyura`, `nyzip` | 2009-04 (original `CopyFeatures` from `CSCL_03222009.gdb`) + 2009-10 (copied into the standing template) - nothing else | Frozen 17 years, zero touches since |
| `nyad`, `nybb` | 2012-09-27 only | Touched once, frozen 13 years since |
| `nyap` | 2012-09-27, then 2015-03-09 | Touched twice, frozen 10+ years since |
| `nypuma2010`, `nypuma2020` | 2026-04-22/23 only | Current - but because 2020-census-geography layers have no history before this cycle, not because of an active refresh cadence |

Six layers share the exact same October 2009 `CopyFeatures` batch, sourced from
`\\NEWWAVEGEO\D$\Src\bownegroup\CSCL\Phase II\ETL\Templates\District Boundary.gdb` on Windows XP -
their FGDC metadata template's own placeholder text was never filled in ("REQUIRED: A brief
narrative summary of the data set."), and no later sync/mod event of any kind is recorded for
them, ever. `nyad`/`nybb`/`nyap` show the toolchain *is* capable of revisiting a layer (each was
resynced once or twice, years apart) - they just haven't been touched again since their last
recorded event either. `nypuma2010`/`nypuma2020` are the only layers with a genuinely current
date, and that reflects a new census geography needing a first-ever build, not an active refresh
habit.

**This directly explains an already-known, previously unexplained oddity.**
`poc_validation/compare_gdb.py`'s `KNOWN_STRUCTURAL_DIFFS` comment notes: "Prod's `nyura` carries
a stale copy of `nybid`'s schema; both are empty." `nyura` and `nybid` are in the *same* frozen
2009-10-20 batch - literally sibling `CopyFeatures` calls from the same ancient template session,
never independently rebuilt since. That's the mechanism behind the schema collision, not a
coincidence.

## Root Cause

Not a bug in either ETL - a genuine characteristic of this legacy toolchain: a layer gets rebuilt
when someone notices it needs to be (new census geography, a schema change), and otherwise rides
along as the same underlying FileGDB object, re-exported but not regenerated, for over a decade.
`altnames` and the six 2009-batch district layers are just the layers nobody has had a reason to
touch since the original CSCL ETL was built.

## Impact Assessment

Not independently actionable - this is corroborating evidence, not a new discrepancy to fix.
Strengthens the case (for the GR conversation) that `Bug 011`'s `gdb_altnames` finding isn't an
isolated fluke: the same "set up once, never revisited" pattern is visible, mechanically, across
at least two different CSCL-family products going back to two different tooling eras (2009 XP/
ArcGIS 9.3 for the District layers, and separately for `altnames` in the LION gdb).

## How to reproduce

```bash
# Modern "GDB_Items" catalog (LION gdb and similar recent-format FileGDBs):
ogrinfo -ro <path-to-gdb-or-vsizip-path> -sql \
  "SELECT Name, Documentation FROM GDB_Items WHERE Name IN ('layer1','layer2')"
# grep the Documentation blob for <CreaDate>/<ModDate>/<SyncDate> and <Process ToolSource=...>

# Classic FileGDB (older format, e.g. the District gdb): find GDB_UserMetadata by probing
# a0000000*.gdbtable files directly until one exposes Name/Xml columns:
for f in <gdb-dir>/a0000000*.gdbtable; do ogrinfo -ro "$f" 2>/dev/null | head -1; done
ogrinfo -al <gdb-dir>/<the-matching-file>.gdbtable
```

## References

- [Bug 011](./011-altnames-saf-replicant-join-ids.md) - the content-level `gdb_altnames` finding
  this corroborates
- `poc_validation/compare_gdb.py`'s `KNOWN_STRUCTURAL_DIFFS` - the `nyura`/`nybid` note this
  explains
- Local files inspected: `/Users/alexrichey/Downloads/nyclion_26b.zip`,
  `/Users/alexrichey/Downloads/nyclion_26c.zip`, `/Users/alexrichey/Downloads/v26B_Districts.gdb`
