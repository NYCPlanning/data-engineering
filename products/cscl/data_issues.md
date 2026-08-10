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
| [CSCL-DISTRICTS-01](#cscl-districts-01) | District gdb | `nymcea` fragments to 249 parts | Open | 26b |
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

**What would settle it:** a decision with GR on how we handle them.

### CSCL-LION-07

**Center of curvature** · Watch · Last verified 26a

All of these were resolved for 25d by linearizing geoms with a very small tolerance. At
least one returned in 26a.

Related: the working theory for [CSCL-DISTRICTS-02](#cscl-districts-02) is the same
`linearize()` mechanism, so the two may resolve together.

**What would settle it:** a decision with GR — play whack-a-mole per release, or agree the
difference is small enough to accept.

## District gdb

### CSCL-DISTRICTS-01

**`nymcea` fragments to 249 parts (prod: 122)** · Open · Last verified 26b

Prod is singlepart: clipping splits some MCEAs into disjoint pieces and prod writes each as
its own feature, so 115 dissolved (borough, MCEA) groups become 122 features. Our output
splits the same groups into **249** parts at the shoreline clip.

These aren't slivers — all residual parts are ≥100 sq ft, and the sub-100-sqft filter in
`clipped_geom` already gives exact prod part counts on `nycb2010/2020`, `nyct2010/2020`,
`nyed` and `nypuma2020`. Total area matches prod (+0.000%) and attributes are correct.

We deliberately did **not** tune a per-layer threshold to force 122; that would be fitting
noise.

**What would settle it:** whoever owns MCEA answering whether prod applies a larger minimum
mapping unit for it, or dissolves *after* clipping.

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
