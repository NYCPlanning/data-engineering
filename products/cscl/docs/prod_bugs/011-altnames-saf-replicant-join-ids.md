# Bug 011: `gdb_altnames` Join_ID Coverage Gap - SAF-Replicant Join_IDs Were Entirely Missing

**Status:** Confirmed prod-side staleness at scale (2026-09-19) - not a code bug, root cause
understood; not fixable on our side
**Affected Output:** `gdb_altnames` (LION gdb, `nyclion_*.zip`)
**Severity:** High - the largest single diff category in the whole CSCL project (~40,000 of
`qa__diffs_all`'s 72,039 rows)
**Discrepancy Count:** Distinct `Join_ID` coverage improved from 16,617/32,867 (~51%) to
30,820/33,246 (~93%). With `qa__diffs_fgdb_altnames`/`qa_int__prod_fgdb_altnames` now in place
(exact full-row-content diff, not the coarser distinct-`Join_ID`-count comparison used
previously): 66,056 total diff rows (51,467 legacy-only, 14,589 build-only) - see the 2026-09-19
update below for the full breakdown.

## Flagship example

`Join_ID 21543702000000N`: our build's current-source row is `BX PSYCHIATRIC CTR CHILDRENS CTR`.
Prod's row at that *exact same* `Join_ID` is `BRONX STATE HOSPITAL ENTRANCE`/
`BX STATE HOSPITAL ENTRANCE`. "Bronx State Hospital" was renamed **Bronx Psychiatric Center** in
the 1970s - institutionally wrong for roughly fifty years. But it's better than that: the
underlying B7SC (`21543702`) has **11** carefully-documented alternate spellings in current
source (`NYC CHILDRENS CENTER`, `NYC CHILDREN'S CTR BX CAMPUS`, etc.) - all created by DCP staff
in a single 13-minute window on **2016-08-31** (14:48:58-15:01:52), clearly one deliberate
data-entry session properly re-surveying this campus. Prod's altnames output has not picked up
that update in the ten years since. Checked whether this points at a missing input source (a
"historical names" layer we're failing to ingest) before concluding staleness - it doesn't:
current source already has both a legitimate non-principal alternate name for the *street*
(`BRONX STATE HOSPITAL ENTRANCE`, `dcp_flag='Y'`, a different B7SC for the actual road) and the
full 2016 building-level rename for the *place*. Nothing is missing on the input side; prod
simply hasn't regenerated this output from source in at least a decade. See the 2026-09-19 update
below for how this generalizes (checked across the full population of ~4,000 problem `Join_ID`s,
not just this one).

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

**`gdb_altnames.sql` change:** split into two parallel paths instead of one shared `b7scs`/`names`
join - `segment_altnames` (unchanged: every non-excluded name variant per implicit B7SC) and
`saf_altnames` (new: `saf_b7scs`, unnested from `int__saf_altnames_join_ids`'s `implicit_b7scs`,
joined against `names` **restricted to `principal_flag = 'Y'`**). See the second bug below for
why the SAF path needed its own, narrower join rather than reusing `segment_altnames`'s.

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

### A second real bug, found via an actual CI build: over-generation of names per SAF Join_ID

A real build (`ar-cscl-26c`, run `35354234855`) with the fix above deployed showed `gdb_altnames`
at **100,940 row diffs (77.8%)** - worse than the 56,017/43.18% baseline before any of this
session's `gdb_altnames` work, despite `Join_ID` coverage having genuinely improved. Traced one
SAF Join_ID generating **84 name rows** on its own (`B7SC 11254001`, "Dr. Ronald E. McNair
Playground" - 84 `FeatureName` spelling variants, exactly 1 `principal_flag = 'Y'` row). The
`saf_b7scs` join was reusing `segment_altnames`'s "every non-excluded variant" `names` join,
correct for real segments (spec §2.7.4 genuinely wants every StreetName/FeatureName variant
there) but wrong for SAF records - the legacy `SetSAFStreetNameAndCode`'s SAF-specific name
lookup uses `GetRow(... AND PRINCIPAL_FLAG = 'Y')`, singular, principal-only. Fixed by giving the
SAF path (`saf_altnames`) its own join restricted to `principal_flag = 'Y'`.

**Effect:** 100,940 (77.8%) → 66,076 (50.9%), dev-only 47,658 → 14,589. Confirmed the regular
segment path's own dev-only count is untouched (1,025, matching the pre-existing 1,023 baseline)
- the fix is isolated to the SAF path as intended.

### The remaining gap looks like more prod staleness, not a code bug

Of the 14,589 dev-only rows post-fix, 13,564 are SAF-derived (only 1,025 are the pre-existing
regular-segment gap). Spot-checked one: dev's row at Join_ID `33543001000000V` is "EAST 14
STREET"; prod's real row at that *exact same Join_ID* is "AVENUE Y". Traced the source: address
point `5158331`'s `B7SC_VANITY = '33543001'` in current source (which does mean "East 14
Street") - matches what the code reads exactly, no bug there. Since the Join_ID is *derived
directly from* the vanity B7SC, this isn't "right key, wrong name" - it means this specific
address point's vanity B7SC has itself been reclassified in CSCL since whatever snapshot prod's
`AltNames` generation is working from. Same staleness shape as `CSCL-SAF-01`
(`Segment_LGC`) and `Bug 010` (`FEATURENAME`) - now apparently also affecting
`AddressPoint.B7SC_VANITY`/`B7SC_ACTUAL`. **Only checked one example** - not yet confirmed
systematically across the 13,564 the way `CSCL-SAF-01`'s LGC pattern was confirmed at full scale.

## Verification

`Join_ID` coverage, tested directly against Postgres with a fresh, independently-verified 26c
prod `fgdb_altnames` copy (loaded via `load_production_lion_fgdb_layers`):

| | before | after |
|---|---|---|
| dev distinct `Join_ID` | 16,617 | 30,825 |
| prod distinct `Join_ID` | ~32,867 (stale estimate) / 33,246 (fresh) | 33,246 |
| overlap | ~16,617 (~51%) | 30,820 (~93%) |

Row-level diff, confirmed via a real CI build (`ar-cscl-26c`, run `35354234855`) both before and
after the principal-only fix above:

| | pre-session baseline | first (buggy) fix | after principal-only fix |
|---|---|---|---|
| Total row diff | 56,017 (43.18%) | 100,940 (77.8%) | 66,076 (50.9%) |
| dev-only | 1,023 | 47,658 | 14,589 (1,025 regular + 13,564 SAF) |
| prod-only | 54,994 | 53,282 | 51,487 |

**Net result: `Join_ID` coverage genuinely improved (51%→93%), but the row-level diff is still
worse than the pre-session baseline (50.9% vs 43.18%).** That's not a sign the fix is wrong -
it's that we're now generating content for ~14,000 previously-entirely-absent `Join_ID`s, and a
large fraction of the underlying `AddressPoint` records appear to have drifted from whatever
CSCL snapshot prod's `AltNames` generation is stuck on (see staleness section above). Marking
this **blocked** rather than fixed until that's confirmed at scale - see Recommendation.

## New ETL Implementation

`models/intermediate/saf/int__saf_altnames_join_ids.sql` (new) +
`models/product/lion/gdb/gdb_altnames.sql` (split into `segment_altnames`/`saf_altnames` paths,
the latter principal-only).

## Recommendation

1. Confirm the `AddressPoint.B7SC_VANITY`/`B7SC_ACTUAL` staleness theory at scale, the way
   `CSCL-SAF-01`'s `LGC`-01 pattern was confirmed across all 9-13 affected rows - sample a larger
   set of the 13,564 SAF-derived dev-only rows and check whether their underlying address point's
   current vanity/actual B7SC resolves to a *different* real street than whatever prod's row at
   that same computed `Join_ID` shows.
2. If confirmed, this folds into the same GR conversation as `CSCL-SAF-01`/`Bug 010` - one more
   data category where prod's output reflects an older CSCL state than the current release.
3. Not yet worth reverting the `commonplace`/`addresspoint` implementation - `Join_ID` coverage
   is a real, durable improvement, and the row-level regression looks like previously-invisible
   staleness surfacing, not new bugs in the join logic itself.

## Update 2026-09-19: root cause confirmed at scale, via new QA models

A queryable diff layer now exists for this specific output: `gdb_altnames_by_field` (dev, keyed by
`_altnames_key = md5(PDir|PType|SName|SType|SDir|Street|Join_ID)` - the full row content, since
this layer has no natural row id), `qa_int__prod_fgdb_altnames` (the same hash over a fresh prod
`fgdb_altnames` copy), and `qa__diffs_fgdb_altnames` (their set difference, now folded into
`qa__diffs_all`). This made it possible to actually answer Recommendation #1 above instead of
extrapolating from one example.

**Row-count-per-`Join_ID` differences are a red herring.** Some `Join_ID`s (e.g. a Bronx subway
line, `2504604010000  `) show prod with 30x more raw rows than dev (458 vs 14) - but that's prod
carrying literal duplicate copies of the same handful of names (one subway line spans many
physical rail segments; prod appears to insert its name set once per segment without deduping,
dev's `SELECT DISTINCT` correctly collapses them). Confirmed this produces **zero** actual diff
rows: the hash-based key already treats duplicate content as identical regardless of how many
times each side repeats it.

**The real, dominant diff is concentrated in `CommonPlace`-sourced SAF Join_IDs (types N and X -
Non-Addressable Placenames), not `AddressPoint` as originally suspected:** N+X account for 39,593
of 51,467 legacy-only rows (77%) and 11,974 of 14,589 build-only rows (82%); every other type
combined is a minority. `V` (AddressPoint vanity) also contributes but at much smaller scale.

**Mechanism, confirmed on concrete examples then verified at full scale:** `Join_ID`
`20079701000000N` decodes (boro 2, streetcode `00797`, LGC `01`) to a B7SC whose *current* source
value is unambiguous - `dcp_cscl_featurename` has exactly one entry there, `BAY PLAZA` (principal)
and `BAY PLAZA SHOPPING CENTER` - which is exactly dev's single row for that `Join_ID`. Prod's row
for the *same* `Join_ID` string has **106 distinct alternate names**, none related to Bay Plaza -
every spelling variant of "Martin Luther King Jr Avenue" and "Bartow Avenue" (`MLK`, `M L KING`,
`REV MARTIN LUTHER KING JR`, `...AVENUE EB ROADBED`, etc). A second example (`11041001000000V`)
showed the same shape: prod's 109 rows are ~109 *different, unrelated* East/West numbered cross
streets, dev's 1 row is the correct current name (`5 AVENUE`).

Checked whether these prod-only clusters correspond to *any* currently-valid entity: for each
problem `Join_ID`, reconstructed its underlying 7-character street-code+LGC suffix and searched
`dcp_cscl_featurename`/`dcp_cscl_streetname` for that suffix under **all 5 possible borough
digits** (since the `CommonPlace`/`AddressPoint` Join_ID formula discards the source B7SC's own
borough digit, substituting the host segment's borough instead - confirmed from
`GetSAFBytesJoinID` above - so the true borough of the original B7SC can't be recovered from the
`Join_ID` string alone, and all 5 possibilities must be checked). Across all 4,064 distinct
problem `Join_ID` suffixes: **zero matched anything in current source, under any borough
interpretation.** This isn't a name that moved to a different borough's B7SC and got missed - the
name clusters prod carries for these `Join_ID`s have no relationship to *any* currently-active
`StreetName`/`FeatureName` entry anywhere in the city.

**The flagship example above** (`Join_ID 21543702000000N`, see top of this doc) has four more
siblings showing the identical pattern: `21543704/705/708000000N`, `27335001000000V`. A second,
independently-dateable one: `Join_ID 21503001000000P`, prod-only, `BRONX TERMINAL MARKET`/
`...BUILDING` - the original market structure at 149th St was demolished and redeveloped into the
Gateway Center mall, which opened in 2009.

**Conclusion:** confirmed, not just for one example but for the entire population of problem
`Join_ID`s. This is the same "prod accumulates decades of historical alias records and never
fully regenerates them" pattern as `CSCL-SAF-01` and `Bug 010`, now shown to be the dominant
driver of the whole-project diff (`qa__diffs_all`, 72,039 rows) once the coverage gap itself was
fixed. Not fixable on our side - we can only regenerate from current source, and prod's data for
these `Join_ID`s doesn't correspond to current source at all. Worth raising with GR as its own,
largest-magnitude example of the staleness pattern, alongside `CSCL-SAF-01`/`Bug 010`.

**Corroborating evidence found in the FileGDB's own metadata (2026-09-19).** Inspected a real
26C `v26C_Lion.gdb` (`ogrinfo`'s `GDB_Items` system table, which carries ArcGIS's per-item ESRI
metadata as embedded FGDC XML). All three layers in the GDB were assembled into this container on
the same day (2026-08-10, per the workspace-level processing lineage:
`CreateFileGDB` → `FeatureClassToGeodatabase node` → `TableToGeodatabase node_stname` →
`TableToGeodatabase altnames`, all within one minute of each other). Their item-level `CreaDate`
fields tell a different story:

| Layer | Item `CreaDate` |
|---|---|
| `node` | 2026-07-28 (current cycle) |
| `node_stname` | 2026-07-29 (current cycle) |
| `altnames` | **2009-01-05** - unchanged |

`node`/`node_stname` both show a `CreaDate` matching this release cycle - they were genuinely
recreated. `altnames`'s `CreaDate` is still January 2009, seventeen years old, sitting right next
to two siblings that were freshly rebuilt the same day. Its `ModDate`/`SyncDate` *are* 2026-08-10
(it was touched during packaging), consistent with "the same underlying table object got copied
into this export" rather than "this table's contents were regenerated." Not proof on its own
(`CreaDate` is a well-known ArcGIS field that rides along unchanged across template/object reuse),
but a concrete, independent data point pointing the same direction as the content-level analysis
above. The embedded FGDC abstract also documents the many-alias-spellings mechanism as
intentional design (its own worked example: "Adam Clayton Powell Boulevard" →
"Powell Boulevard"/"A C Powell Boulevard") - confirming the *mechanism* is real and deliberate,
just evidently not kept in sync with current source for a large fraction of entries.

## References

- `data_issues.md` CSCL-LION-09 (updated with this)
- Legacy source: `~/dev/cscl_etl_archive/ETL/CSCL.ETL.Extractor/Source Files/ExtractorClass.cs`
  (`AddAltNames`, `GetBytesJoinID`, `GetSAFBytesJoinID`, `SetSAFStreetNameAndCode`)
- `models/intermediate/saf/int__saf_segments.sql` - reused SAF-record resolution
- Related: `CSCL-SAF-01`, `Bug 010` (same staleness shape, different source tables)
