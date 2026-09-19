# CSCL Known Bugs

This directory documents known bugs in the legacy CSCL ETL that have been identified through diff analysis between the legacy and new dbt-based implementations.

## Naming Convention

Bug files should follow the pattern:

```
{bug-number}-{short-description}.md
```

Where:
- `{bug-number}` is a zero-padded 3-digit number (001, 002, etc.)
- `{short-description}` is a kebab-case brief description of the bug

## Bug Documentation Template

Each bug document should include:

- **Status:** Fixed/Under Investigation/Won't Fix
- **Affected Output:** Which output files are affected
- **Severity:** Low/Medium/High
- **Discrepancy Count:** How many records show this issue
- **Summary:** Brief description
- **Technical Details:** Detailed explanation with code references
- **Example:** Concrete example with real data
- **Root Cause:** Why the bug exists
- **New ETL Implementation:** How the new ETL fixes it
- **Impact Assessment:** Analysis of the effect
- **References:** Links to relevant documentation

## Marking Diffs as Accounted For

When a bug is documented and understood, update the relevant diff model's `accounted_for` logic to mark these discrepancies:

```sql
-- Bug XXX: Description
-- See: docs/bugs/XXX-description.md
OR change_keys = ARRAY['field_name']::text []
```

## Current Bugs

- [Bug 001](./001-boe-lgc-pointer-two-digit-codes.md): BOE LGC Pointer incorrect for two-digit LGC codes (67 records, all Queens)
- [Bug 002](./002-police-geo-centroid-mismatch.md): Police precinct/sector/patrol borough assignment differs near boundaries due to ESRI vs. PostGIS centroid computation (7 Atomic Polygons citywide)
- [Bug 003](./003-saf-gnx-side-ap-point-mismatch.md): SAF GNX side-AP fields differ near Atomic Polygon boundaries due to ESRI vs. PostGIS point-in-polygon computation
- [Bug 004](./004-randalls-island-missing-zip.md): Randall's Island NSF segments missing zip code in production (10 records, all Manhattan) - GR-confirmed production bug
- [Bug 005](./005-doubly-reversed-protosegments.md): Doubly-reversed protosegments not reversed in production (5 segments, Bronx and Brooklyn) - GR-confirmed production bug
- [Bug 006](./006-jr-reviewed-manual-diffs-aug-2026.md): 14 individually-reviewed diffs across LION and SAF s_generic/s_roadbed (various fields) - JR-confirmed correct via manual spreadsheet review, Aug 2026
- [Bug 007](./007-sept-2026-remaining-diffs-investigation.md): Root-cause investigation of the last 6 unaccounted-for diffs (LION coincident count, SAF flag on a ramp segment, corrupt curve geometry, RPL node swap, SAF lgc codes) - 4 resolved, 1 open pending a stale-prod-load recheck, Sept 2026
- [Bug 008](./008-saf-flag-leaks-onto-ramp-protosegment.md): `special_address_flag` leaks from a centerline record onto its sibling on/off-ramp (`ALT_SEGDATA_TYPE = R`) protosegment in production, contradicting the legacy ETL's own documented rule - 1 record, Queens
- [Bug 009](./009-thined-redistricted-field-mismatch.md): `thined.txt`'s congress/state-senate/municipal-court/city-council district fields diverge from prod for ~2% of rows each, likely a source-vintage mismatch in the `ElectionDistrict` layer - unconfirmed, blocked
- [Bug 010](./010-featurename-normalizing-tables-stale-accretion.md): `Exception.txt`/`Enders.txt`/`SND.txt` contain prod-only entries that don't correspond to any current FEATURENAME/StreetName source record (confirmed, incl. a named example - "Isobel Rooney M.S. 80" now "St Gabriel Church" - shared across Exception and SND) - prod's legacy pipeline appears not to fully regenerate these each cycle - root mechanism confirmed, cause needs GR, blocked
- [Bug 011](./011-altnames-saf-replicant-join-ids.md): `gdb_altnames`'s ~50%-of-prod `Join_ID` coverage gap was missing SAF-replicant Join_IDs (CommonPlace/AddressPoint-sourced) - implementing them raised coverage 51%→93%, but CI showed the row-level diff got *worse* (43%→51%) because a large share of the newly-covered `AddressPoint` records look stale in prod (same shape as `CSCL-SAF-01`/`Bug 010`, now in `B7SC_VANITY`/`B7SC_ACTUAL`) - blocked pending scale confirmation, not closed
- [Bug 012](./012-saf-replicant-lion-rows.md): `gdb_lion` was missing SAF-replicant rows entirely (`SAFStreetName`/`SAFStreetCode` were permanently null because the rows that carry them didn't exist) - implemented for all SAF types, `SpecAddr` counts now match prod exactly for 8/12 types and are short only by the deliberately-deferred continuous-parity duplication for the other 4 - v1, not closed (see doc for known simplifications)
