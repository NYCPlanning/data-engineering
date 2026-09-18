# Bug 011: `gdb_altnames` Join_ID Coverage Gap - SAF-Replicant Join_IDs Were Entirely Missing

**Status:** Fixed (partial - `commonplace`/`addresspoint` branches only, see Scope below)
**Affected Output:** `gdb_altnames` (LION gdb, `nyclion_*.zip`)
**Severity:** High - was the largest unexplained magnitude in the LION gdb
**Discrepancy Count:** Distinct `Join_ID` coverage went from 16,617/32,867 (~51%) to
30,820/33,246 (~93%) against a fresh, real prod comparison.

## Summary

`CSCL-LION-09` documented dev producing roughly half of prod's distinct `Join_ID` values in
`gdb_altnames`, with the mechanism unknown - ruled out SAF-replicant scope as an explanation at
the time because it was measured against the wrong thing (segment *count*, not `Join_ID`
*coverage*) and no legacy source was available for `gdb_altnames` specifically.

This round, the user got GR to point at the actual legacy ETL source (a full C# codebase,
`~/dev/cscl_etl_archive`, distinct from the smaller Python script drop used for `VIntersect`/
`node_stname` earlier). The real algorithm lives in `ExtractorClass.cs` (the shared base class
both `LIONExtractorClass` and `GDBExtractorClass` inherit from - **not** either of those two
subclasses, which is where an earlier pass in this session looked and wrongly concluded nothing
was there): `AddAltNames()`, `GetBytesJoinID()`, `GetSAFBytesJoinID()`.

**Root cause: SAF-replicant Join_IDs use a completely different encoding and source data than
non-SAF Join_IDs, and `gdb_altnames.sql` only ever produced the non-SAF kind** (already flagged
in the model's own comment - "SAF-replicant Join_IDs ... are not yet produced"). Two of the
three SAF source types (`CommonPlace`, `AddressPoint`) have **no relationship at all** to their
associated segment's own street classification - a CommonPlace (park, plaza, etc.) can point at
a segment while carrying a totally unrelated B7SC - so the regular per-segment LGC-based
`Join_ID`/name derivation could never produce them no matter how correct it is.

## Technical Details

### The real algorithm (`ExtractorClass.cs`)

`AddAltNames(extract, feature)` runs once per LION output record (every Centerline,
NonStreetFeature, Rail, Shoreline, Subway row), using that record's own output LGC1-4 fields
(`L6`-`L9`) and B5SC (`L1`+`L5`) to look up `StreetName`/`FeatureName` by B7SC, exactly matching
what `gdb_altnames.sql` already did. Dedup is a running in-memory list
(`_altNamesTableList`, initialized once for the whole run) keyed on `Join_ID + name`.

Separately, `GetSAFBytesJoinID()` computes a **different** 15-byte Join_ID for SAF-replicant
records, branching on `extract.SAFDataFeature.TableName`:

- **`altsegmentdata`**: boro + `B5SC` (right 5, i.e. street-code only) + that row's own
  `LGC1`-`LGC4` + `SAFType`.
- **`commonplace`**: boro + `B7SC`'s street-code + `B7SC`'s own LGC + `00`+`00`+`00` + `SAFType`.
- **`addresspoint`**: boro + `B7SC_VANITY`/`B7SC_ACTUAL` (chosen by `SPECIAL_CONDITION` = `'V'`/
  `'S'`) street-code + that B7SC's own LGC, then up to 3 more LGCs from `ADDRESSPOINTLGCS`
  (excluding `BOE_LGC = 'Y'`) + `SAFType`.

Traced `SetSAFStreetNameAndCode()` (the other SAF-specific method, runs just before `AddAltNames`
on a SAF-replicant row) to check whether it overwrites `L1`/`L5`/`L6`-`L9` to match the SAF row's
own B5SC/LGCs before `AddAltNames` reads them - **it doesn't**. It only ever sets
`SAFStreetName`/`SAFStreetCode`/zip fields. This is the basis for the scope decision below.

### Scope decision: `altsegmentdata` excluded, `commonplace`/`addresspoint` implemented

Since `AddAltNames` always reads the segment's own regular `L6`-`L9`, and `altsegmentdata`
SAF-replicant rows (protosegments) plausibly populate those same fields with their own
alt-segment B5SC/LGCs (unverified, but this is the null hypothesis given no code disproves it),
implementing `GetSAFBytesJoinID`'s `altsegmentdata` branch on top risks emitting a redundant,
differently-keyed duplicate of Join_IDs the regular path may already produce - not a real gap.
`commonplace` and `addresspoint`, by contrast, have B7SCs with no relationship to any segment's
regular LGC list, so nothing else could ever produce them.

**New model:** `models/intermediate/saf/int__saf_altnames_join_ids.sql` - unions
`commonplace`/`addresspoint`-sourced Join_IDs, reusing `int__saf_segments`'s existing SAF-record
resolution (roadbed-pointer-list generic mapping, inclusion via `int__lion`) rather than
re-deriving it. Outputs `(segmentid, join_id, implicit_b7scs[])`.

**`gdb_altnames.sql` change:** the `b7scs` CTE now unions `segment_b7scs` (unchanged) with a new
`saf_b7scs` CTE that unnests `int__saf_altnames_join_ids`'s `implicit_b7scs` array, before the
existing `names` join - no other change to the model.

### A real bug found and fixed during implementation

B7SC is 8 bytes: a 6-byte B5SC (borough digit + 5-digit street code) + 2-byte LGC - confirmed
directly against data (`dcp_cscl_streetname.b7sc = '11001001'` → B5SC `'110010'`, LGC `'01'`).
First implementation attempt used `left(b7sc, 5)` for the B5SC portion (assuming a 5-byte street
code with no borough digit, matching the Join_ID string's *own* street-code segment, which
deliberately strips the borough digit since it's supplied separately). That's wrong for
reconstructing the *full* B7SC needed for the name lookup - it silently produced B7SCs that
matched **zero** real `StreetName`/`FeatureName` rows (0 of 9,645 candidate B7SCs matched, and a
dev/prod Join_ID comparison showed almost no improvement: 16,632 vs the original 16,617).
Fixed to `substring(b7sc, 2, 5)` for the Join_ID's street-code segment (skip the borough digit,
matching the legacy code's `b7sc.Substring(1,5)`) and the *full* B7SC (`left(b7sc, 6)` + LGC, or
just the B7SC itself for the row's own LGC slot) for the `implicit_b7scs` used in the name
lookup. After the fix: 14,105 of 14,108 candidate B7SCs matched a real name (99.98%).

## Verification

Tested directly against Postgres (not yet run through a full CI build) with a fresh,
independently-verified 26c prod `fgdb_altnames` copy (loaded this session via
`load_production_lion_fgdb_layers`, not the stale copy an earlier investigation round may have
used):

| | before | after |
|---|---|---|
| dev distinct `Join_ID` | 16,617 | 30,825 |
| prod distinct `Join_ID` | ~32,867 (stale estimate) / 33,246 (fresh) | 33,246 |
| overlap | ~16,617 (~51%) | 30,820 (~93%) |

Closes roughly 85% of the original gap. Remaining ~2,426 uncovered `Join_ID`s are most likely
the `altsegmentdata` branch (deliberately not implemented, see Scope above) plus SAF records
whose B7SC didn't pass the `length(b7sc) = 8` guard or genuinely have no matching name - not yet
individually characterized.

**Not yet verified:** a full CI build + `compare_gdb.py` run (only checked directly in Postgres
against a manually-materialized copy of the new logic). Do that before treating this as closed,
and check whether the `SType`/`Join_ID` diversity flags in `compare_gdb.py`'s per-column report
clear as expected.

## New ETL Implementation

`models/intermediate/saf/int__saf_altnames_join_ids.sql` (new) +
`models/product/lion/gdb/gdb_altnames.sql` (extended `b7scs` CTE).

## References

- `data_issues.md` CSCL-LION-09 (updated with this fix)
- Legacy source: `~/dev/cscl_etl_archive/ETL/CSCL.ETL.Extractor/Source Files/ExtractorClass.cs`
  (`AddAltNames`, `GetBytesJoinID`, `GetSAFBytesJoinID`, `SetSAFStreetNameAndCode`)
- `models/intermediate/saf/int__saf_segments.sql` - reused SAF-record resolution
