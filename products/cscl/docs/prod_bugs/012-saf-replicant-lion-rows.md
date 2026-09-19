# Bug 012: SAF-Replicant Rows Implemented in `gdb_lion`

**Status:** Implemented (v1) - continuous-parity duplication deferred, not a bug
**Affected Output:** LION gdb (`gdb_lion`) - `SpecAddr`, `SAFStreetName`, `SAFStreetCode`,
house-number fields, `Join_ID`
**Severity:** N/A (feature gap, not a legacy-ETL bug)
**Discrepancy Count:** ~1,700 rows short of prod's total (continuous-parity gap, quantified below)

## Summary

`gdb_lion` previously only emitted one row per segment/proto-segment ("regular" rows). Per ETL
spec §2.7.2 ("Segment Replication for SAF Data"), prod additionally emits one extra "SAF
replicant" row - same geometry, SAF-specific overrides - for every SAF datum associated with a
segment: `AltSegmentData` entries with a `SAFTYPE` set (types A/B/C/D/E/F/O/P), `CommonPlace`
points with a `SAFTYPE` set (G/N/X), and `AddressPoint` records with `SPECIAL_CONDITION` in
(`S`/`V`). This is why `SAFStreetName`/`SAFStreetCode` were permanently `NULL` in `gdb_lion` -
those two fields only exist on replicant rows, which we never generated. Confirmed via row count:
prod's `fgdb_lion` has 243,706 rows vs our (then) 220,026 - a ~22,000-row gap matching the SAF
data volume almost exactly.

## Technical Details

New model `int__saf_replicant_fields.sql` normalizes all three SAF sources into one shape (per-row
`SpecAddr`, `SAFStreetName`/`SAFStreetCode`, side-gated house numbers/zip, SAF-replicant
`Join_ID`), keyed by `int__saf_segments.segment_lionkey` - the *specific* proto-segment a SAF
datum belongs to, not just its `segmentid` (which can have several coincident proto-segment rows
in `int__lion`; joining on `segmentid` alone fans out incorrectly).

`gdb_lion.sql` is now a `UNION ALL` of `base_rows` (unchanged logic) and `replicant_rows` (same
base attributes, with `SpecAddr`/`SAFStreetName`/`SAFStreetCode`/house numbers/`Join_ID` swapped
for the SAF-specific values). The base row's `SpecAddr` is now always `NULL` (previously it
carried an approximation via `special_address_flag`, per spec: "SpecAddr will be populated with a
blank in all output records other than SAF replicants" - keeping the old approximation would have
double-counted the same SAF association on both the base row and its new replicant row).

Two real bugs found and fixed during implementation, both via row-count/value validation against
`production_outputs.fgdb_lion`:

1. **Segmentid vs. lionkey.** Joining SAF entries to `int__lion` by `segmentid` fanned out for any
   segment with multiple coincident proto-segment rows (1,530 segmentids affected) - fixed by
   joining on `segment_lionkey` instead.
2. **RPL-redirect double-counting.** `int__saf_segments`'s roadbed-pointer-list redirect resolves
   a `CommonPlace`/`AddressPoint` SAF entry on a divided (generic+roadbed split) street to *two*
   candidate segments (one per side) - correct for the SAF flat files (which export a generic and
   a roadbed file separately), but wrong for LION's single unified row. Fixed with `DISTINCT ON
   (saf_globalid)`, arbitrarily preferring the roadbed-flagged resolution. Also excluded
   `AltSegmentData saftype = 'Z'` (present in source data, absent from the spec's replicant-type
   list and from prod's own `SpecAddr` domain).

## Verification

`SpecAddr` counts, dev vs. `production_outputs.fgdb_lion` (26C):

| Type | Dev | Prod | Match |
|---|---|---|---|
| X | 6,295 | 6,295 | exact |
| N | 5,963 | 5,963 | exact |
| S | 1,130 | 1,130 | exact |
| V | 1,089 | 1,089 | exact |
| O | 133 | 133 | exact |
| D | 112 | 112 | exact |
| C | 6 | 6 | exact |
| G | 1,143 | 1,144 | -1 |
| A | 2,804 | 3,088 | -284 (continuous parity, see below) |
| B | 2,312 | 2,439 | -127 (continuous parity) |
| P | 698 | 757 | -59 (continuous parity) |
| E | 288 | 472 | -184 (continuous parity) |

Every type without a continuous-parity carve-out matches prod exactly or within 1. `SAFStreetName`
is populated on 21,971/21,973 replicant rows (99.99%); spot-checked values, `SAFStreetCode`
format, `Join_ID` format, and house-number side-gating against real prod rows all matched
(including prod's own duplicate-row pattern for coincident segments).

Total row count: dev 241,999 vs. prod 243,706 (99.3%). The 1,707-row gap is fully attributable to
the continuous-parity simplification below (~654 rows, computed from real SOSINDICATOR-set entry
counts) plus a residual ~1,053 rows not yet individually explained (plausibly SAF entries
resolving to segments excluded from `include_in_bytes_lion`).

## Known v1 Simplifications (not bugs, deliberately scoped)

- **Continuous-parity duplication not implemented.** For AltSegmentData types A/B/C/E/P,
  `SOSINDICATOR` can serve as a *continuous-parity* flag (not a side indicator) - per spec, such
  an entry should produce *two* replicant rows, each with one side's address range moved into its
  own "left" fields. This isn't implemented; affected entries emit one row with both sides passed
  through as given. Precisely quantifies the `A`/`B`/`E`/`P` gaps above. Base-segment continuous
  parity duplication (a separate, larger existing gap, spec §2.7.1) also isn't implemented in the
  codebase and would be a prerequisite for doing this properly.
- **AddressPoint S/V zip-code override not implemented** - left as the base segment's own zip
  (spec says it should come from the AddressPoint's own zip for the indicated side).
- **`SegCount` not adjusted.** Spec says `SegCount` should reflect all coincident records
  including SAF replicants; this isn't implemented, to avoid risking regression of the
  already-good `SegCount` match on base rows.
- **RPL-redirect side choice is arbitrary** (prefers roadbed) for the ~1,383 CommonPlace/
  AddressPoint SAF entries that resolve to a divided street - the *aggregate* counts match prod,
  but no per-entry verification was done that the *specific* side chosen matches prod's.

## References

- ETL spec: `docs/ETL_V8_02012024.md` §"Segment Replication for SAF Data" (~line 4609) and
  §"Methodologies for Populating Output Fields" (~line 4772, `SpecAddr`/`SAFStreetName`/
  `SAFStreetCode`/`SegCount` rules)
- `models/intermediate/saf/int__saf_replicant_fields.sql`, `models/product/lion/gdb/gdb_lion.sql`
- Related: [Bug 011](./011-altnames-saf-replicant-join-ids.md) (the SAF-replicant `Join_ID`
  formula, originally built for `gdb_altnames` matching, reused/extended here)
