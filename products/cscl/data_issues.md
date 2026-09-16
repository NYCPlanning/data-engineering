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
| [CSCL-LION-06](#cscl-lion-06) | LION | Coincident segments | Open | 26b |
| [CSCL-LION-07](#cscl-lion-07) | LION | Center of curvature | Watch | 26a |
| [CSCL-LION-08](#cscl-lion-08) | LION | `VIntersect` hardcoded null in `gdb_node` | Open | 26b |
| [CSCL-LION-09](#cscl-lion-09) | LION | `node_stname`/`altnames` abbreviation mismatches | Open | 26b |
| [CSCL-LION-10](#cscl-lion-10) | LION | `LegacyID` real-value mismatches on ~2% of segments | Open | 26b |
| [CSCL-DISTRICTS-01](#cscl-districts-01) | District gdb | Shoreline-clip part counts differ (`nymcea`, `nypuma2010/2020`, `nynta2020`) | Open | 26b |
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

**Coincident segments** · Open · Last verified 26b

Some remain.

One instance is diagnosed in detail in
[docs/prod_bugs/007-sept-2026-remaining-diffs-investigation.md](./docs/prod_bugs/007-sept-2026-remaining-diffs-investigation.md#1-coincident_seg_count-lion_dat_brooklyn-304118101066):
segmentid 8101066 (Brooklyn) has a centerline and *two* distinct `rail_and_subway` segments
(itself plus 8105519) all coincident at the same point. Our count of 3 (self + 2 others) is
marked `accounted_for` for that specific `_lion_key`, but this doesn't settle the general
question - it's one data point suggesting prod may not count two same-feature-type duplicates
as two separate coincidences.

**What would settle it:** a decision with GR on how we handle them.

### CSCL-LION-07

**Center of curvature** · Watch · Last verified 26a

All of these were resolved for 25d by linearizing geoms with a very small tolerance. At
least one returned in 26a.

Related: the working theory for [CSCL-DISTRICTS-02](#cscl-districts-02) is the same
`linearize()` mechanism, so the two may resolve together.

**What would settle it:** a decision with GR — play whack-a-mole per release, or agree the
difference is small enough to accept.

### CSCL-LION-08

**`VIntersect` hardcoded null in `gdb_node`** · Open · Last verified 26b

Prod's `node` layer populates `VIntersect` (`''` or `'VirtualIntersection'`) on effectively
every row; our `gdb_node.sql` hardcodes it to `NULL::text` because the source Node file has
no field to derive it from (see the TODO there). Row-level comparison flags ~139,700 of
~142,800 node rows as "modified" as a result — by construction, not because of a data bug.
`poc_validation/compare_gdb.py`'s `KNOWN_STRUCTURAL_DIFFS` now tags `node` so the diff report
reads this as understood rather than a fresh regression each run.

**What would settle it:** finding (or deriving) a source for virtual-intersection status —
possibly inferable from node degree / LGC combinations at each node, but unconfirmed.

### CSCL-LION-09

**`node_stname`/`altnames` abbreviation mismatches** · Open · Last verified 26b

Nothing in `design_doc.md` covers node/node_stname/altnames abbreviation - checked again
this cycle, broadly (not just literal string search): the one adjacent passage (the
Exception Table's "examine last word for possible deletion" rule) is itself struck through
with "TODO this seems to not be done at all? Talk to GR/GSS". `gdb_node_stname.sql`'s own
header already said this was "derived empirically" - there's no written rule to follow here,
only prod's actual output to reverse-engineer against.

**`node_stname` fix (2026-09-16):** `STNAME` only abbreviated a name's *last* word (via
`dcp_cscl_lastword`'s `standard_abbreviation`); prod also abbreviates directional first
words, and uses a different abbreviation for several last words entirely. Root-caused by,
for every node with a matching prefix on both sides, recording what suffix prod actually
used - see `seeds/node_stname_lastword_overrides.csv` and `gdb_node_stname.sql`'s module
comment for the mechanics. This took `node_stname`'s dev-only row count from 66,649 to
15,888 (76% reduction on that side; the reported "133,287 rows differ" should drop by
roughly the same proportion). Examples now fixed:

| Ours (before) | Prod | Mechanism |
|---|---|---|
| `EAST 174 ST` | `E 174 ST` | first-word directional (`dcp_cscl_universalword`) |
| `CROSS BRONX EXPWY` | `CROSS BRONX EXPY` | last-word override (not in any `dcp_cscl_lastword` column) |
| `UNIVERSITY HEIGHTS BRG SHL` | `UNIVERSITY HEIGHTS BRG SHORELINE` | last-word override (prod essentially never abbreviates SHORELINE - 9,084 vs 113 city-wide) |
| `PELHAM PY HOUSES PEDESTRIAN PTH` (partial) | `PELHAM PKWY HOUSES PEDESTRIAN PATH` | not fixed - `PY` is baked into the raw source `lookup_key` already, see below |

**Known remaining patterns, not fixed** (this is a lexical/word-level substitution; these
need positional or semantic context a per-word table can't express):
- **Directional words are contextual, not absolute.** `WEST` is abbreviated in ~85% of
  cases (`WEST 42 ST` → `W 42 ST`) but not in proper nouns like `WEST FARMS RD`; `EAST
  RIVER` is never abbreviated even though standalone `EAST` usually is. A blanket
  first-word rule can't distinguish "directional modifier" from "part of a proper name."
- **Trailing single-letter designators hide the real last word.** `AVENUE M`/`N`/`S`
  (Brooklyn's lettered avenues) never get `AVENUE`→`AVE` applied, because the regex-based
  "last word" is the letter (`M`), not `AVENUE`.
- **Some raw `lookup_key` source text is already partially abbreviated** (e.g. `PELHAM PY
  HOUSES...` where `PARKWAY` is already stored as `PY`, not `PARKWAY`) - forward-applying an
  abbreviation table can't fix text that's already abbreviated differently than prod chose.
- Standalone generic names sometimes stay unabbreviated where the same word is abbreviated
  as a suffix (`DRIVEWAY` alone stays `DRIVEWAY`; `... DY` when it follows a proper name).

**What would settle it (node_stname):** GR confirming whether prod's abbreviation source is
a superset of `dcp_cscl_lastword`/`dcp_cscl_universalword` or built from an entirely
different word list, and whether the contextual/proper-noun exceptions above are worth a
curated exception list.

**`gdb_altnames`'s `Join_ID` gap** is a separate, larger-magnitude issue with a different
root cause (not an abbreviation problem - see below).

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

### CSCL-LION-10

**`LegacyID` real-value mismatches on ~2% of segments** · Open · Last verified 26b

`gdb_lion.sql` computed `LegacyID` as `lpad(legacy_segmentid::text, 7, '0')`, which produces
SQL NULL whenever `legacy_segmentid` is null. Prod's convention for "no legacy ID" is the
literal string `'0000000'`, not blank - so this alone accounted for the vast majority of a
large `LegacyID` null-rate gap flagged in `gdb_lion`'s per-column report. Fixed by
`coalesce`-ing to `0` before padding.

After that fix, on the ~161,000 segments where we have a real (non-`0000000`) `LegacyID`,
160,889 match prod exactly and 156 are null in dev where prod has a real value - both small.
But **3,170 segments (~2%) have a real `LegacyID` on both sides that simply disagrees**
(e.g. dev `0039154` vs prod `0061538` for the same `LBoro/FaceCode/SeqNum`). This is a
genuine content difference, not a representation artifact, and hasn't been investigated -
worth checking whether `legacy_segmentid` itself drifted between whatever source vintage
prod's legacy ID reflects and our current CSCL extract.

**What would settle it:** picking a handful of the 3,170 mismatched segments and checking
whether their `legacy_segmentid` value changed in a more recent CSCL release, or whether
our lookup is joining to the wrong record for them.

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

**What would settle it:** whoever owns these district layers answering whether prod applies a
larger minimum mapping unit for large/coastal aggregates, or dissolves *after* clipping. The
`nynta2020` reverse-direction case suggests the answer may not be uniform across layers.

### CSCL-DISTRICTS-02

**Sub-0.5% area deltas on unclipped passthroughs** · Open · Last verified 26b

`nyhez` −0.454%, `nycdwi` −0.397%, `nypp` +0.341%. These layers aren't shoreline-clipped, so
clipping can't explain the shift.

Working theory is `linearize()` coercing curved source geometry that prod preserves — the
same mechanism as [CSCL-LION-07](#cscl-lion-07). **Unverified.**

**What would settle it:** confirming the cause. A 0.4% area shift on straight passthrough
data isn't obviously benign, so this should be checked before sign-off rather than accepted.

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
