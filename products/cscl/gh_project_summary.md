# CSCL GitHub issue tracker — what's there, and what to pull into code

Summary of `NYCPlanning/data-engineering`'s CSCL-labeled issues (`label:db-cscl`, 76 issues
as of 2026-09-16), written to answer two questions: what output groups make up CSCL, and
which parts of this GitHub-issue "documentation" have real content worth migrating into
`seeds/lion_outputs.csv` / `data_issues.md` — the actual documentation-as-code homes for
this information.

> [!WARNING]
> **Treat everything below as a lead, not a fact.** These are project-management issues,
> written informally, by people mid-investigation, months to a year+ ago in some cases.
> Some are stale, some are speculative, and at least one direct contradiction is already
> visible (#1989 below). Before acting on anything here, verify it against real build data
> or the actual legacy source, the same way every fix earlier in this project was verified.
> This file exists so you don't have to re-read 76 issues to find the leads — it is not
> itself a source of truth.

## The output group taxonomy

Per the top-level epic, [**#1816 "CSCL ETL"**](https://github.com/NYCPlanning/data-engineering/issues/1816),
CSCL's outputs split into two kinds — files that feed NYC's Geosupport system, and
everything else — and the epic explicitly organizes the Geosupport files into three
"buckets" plus a fourth non-Geosupport bucket. GitHub's sub-issue links (not just body text)
confirm the grouping:

| Group (epic's term) | Umbrella issue | Sub-issues (each closed, "done" as of writing) | Our `file_group`(s) today |
|---|---|---|---|
| **LION flat files** | [#1820](https://github.com/NYCPlanning/data-engineering/issues/1820) | architecture, shoreline segments, rail/subway segments, non-street features, protosegments, coincident segment count, error log, GR/GSS review | `LION flat files` |
| **Non-LION Geosupport — Bucket 1** | [#1843](https://github.com/NYCPlanning/data-engineering/issues/1843) | Special Address File ([#1836](https://github.com/NYCPlanning/data-engineering/issues/1836)), Street Name Dictionary ([#1827](https://github.com/NYCPlanning/data-engineering/issues/1827)), Geosupport Normalizing Tables ([#1828](https://github.com/NYCPlanning/data-engineering/issues/1828)), Face Code File ([#1832](https://github.com/NYCPlanning/data-engineering/issues/1832)), SEDAT/SpecialSEDAT ([#1838](https://github.com/NYCPlanning/data-engineering/issues/1838)) | `SAF`, `SND`, `Normalizing Tables`, `Face Code File`, `SEDAT` |
| **Non-LION Geosupport — Bucket 2** | [#2195](https://github.com/NYCPlanning/data-engineering/issues/2195) | Roadbed Pointer List ([#1839](https://github.com/NYCPlanning/data-engineering/issues/1839)), District Equivalency Files ([#1833](https://github.com/NYCPlanning/data-engineering/issues/1833)), CDTA ([#2214](https://github.com/NYCPlanning/data-engineering/issues/2214)), NTA ([#2215](https://github.com/NYCPlanning/data-engineering/issues/2215)), THINED+NTA ([#2443](https://github.com/NYCPlanning/data-engineering/issues/2443)) | `RPL`, `District Equivalency`, `Census` |
| **Non-GeoSupport File Outputs** | [#2196](https://github.com/NYCPlanning/data-engineering/issues/2196) | **LION gdb** ([#1821](https://github.com/NYCPlanning/data-engineering/issues/1821)), **District Boundary gdb** ([#1840](https://github.com/NYCPlanning/data-engineering/issues/1840)), LION Differences File / LDF ([#1873](https://github.com/NYCPlanning/data-engineering/issues/1873)), gdb downstream requirements ([#2045](https://github.com/NYCPlanning/data-engineering/issues/2045)) | `gdb` (2 subgroups), `LION Difference Files` |

**So: yes, two FGDBs** — "LION gdb" (aka "Bytes LION", published as `nyclion_{version}.zip`
on the [DCP LION page](https://www.nyc.gov/content/planning/pages/resources/datasets/lion))
and "District Boundary gdb" (`v{version}_Districts.gdb.zip`) — both filed, per #2196's own
sub-issue placement, under the "non-Geosupport" bucket rather than as their own top-level
category, even though the epic's prose lists them as a separate bullet from the three
Geosupport buckets. Interesting that these are treated as unrelated to Geosupport at all,
which matches our own pipeline's split (`product/lion` vs `product/districts`).

Our `file_group`/`subgroup` values in `seeds/lion_outputs.csv` already line up with this
almost exactly — the main gap is naming granularity (we have five separate top-level groups
where GH has one "Bucket 1" umbrella, etc.) which is a difference in organization, not a
missing group. **Nothing here suggests a whole output group is unaccounted for in our
seed.**

## Concrete content worth pulling into code

Roughly ranked by how directly it can shortcut work we're already doing:

### 1. `#2054` — the `node`/`node_stname` layers come from an *external, separate* tool

> "Tool in this case is a standalone python script that uses esri packages... There is no
> documentation for this specific transformation, but JD will share the code."

This explains why `design_doc.md` has zero coverage of `node_stname`/`altnames`/`node`
abbreviation and `VIntersect` (confirmed again this session, broadly, not just literal
string search — see `CSCL-LION-09`'s note). It's not an oversight in the main ETL spec;
these come from a *different, undocumented* GR/GSS postprocessing step that:
- deletes "floating" nodes (nodes not connected to any segment)
- generates the node→streetname lookup, "only take preferred streetnames"
- flags `Y`-preferred-LGC records with no B7SC
- flags segments in the intersections table missing from CSCL

**Action worth taking**: get that script from JD. If it contains the actual STNAME
abbreviation logic, it could replace the statistical reverse-engineering this session did
for `CSCL-LION-09` with the real algorithm — and the "floating node" deletion rule may
explain some of `gdb_node`'s remaining dev-only rows we haven't chased down.

### 2. `#2616` — precise root cause + fix for `CSCL-LION-06` (coincident segments)

Our entry currently just says "some remain" with one diagnosed instance. #2616 has an
actual fix:

> Legacy `GetCoincidentSegmentCount()` scopes its "same type" check to the literal ArcGIS
> feature class - Subway and Rail are separate there, so a Subway/Rail crossing is never
> counted as coincident. Our `int__noncenterline_coincident_segments.sql` merges them into
> one `feature_type = 'rail_and_subway'`, so a subway/rail crossing gets double-counted.
> **Fix: match "same type" on `source_table` instead of the merged `feature_type`.**

This is a specific, actionable, well-scoped code fix, not a "needs GR" item — worth doing
directly rather than treating `CSCL-LION-06` as GR-blocked.

### 3. `#1989` — root cause for `CSCL-LION-07` (curve flag / center of curvature), and it *contradicts* our doc

Our entry says "resolved for 25d... returned in 26a," framing it as unexplained recurrence.
#1989 has a concrete mechanism: raw shoreline geometry contains `MULTICURVE`/`CIRCULARSTRING`
segments (true curves, not polylines); `geopandas.read_file` (and parquet round-tripping)
silently flattens these to `MULTILINESTRING`, losing the curve information before our
`numpoints > 2` check ever runs. ~200 records affected, repro'd on segmentid `195828`.
Open question in the issue itself: whether fixing this requires reading geometry via
`fiona` instead of `geopandas` (and whether that breaks elsewhere, e.g. geoparquet reads).
**This means our doc's "resolved... at least one returned" framing is wrong** — it was
never actually fixed, just not fully characterized. Worth rewriting `CSCL-LION-07` with
this mechanism instead of the vaguer "Watch" framing.

### 4. `#2193` — a **known, accepted prod bug we deliberately replicate** with no entry anywhere

> "For seglocstatus, the prod tool incorrectly uses 2010 census tracts instead of 2020.
> We've matched prod for now, but this should be updated."

This is exactly the shape of thing `data_issues.md` exists for (a documented, deliberate
choice to match a known prod defect) and it currently has **no entry at all**. Worth adding
as a new `Accepted` status item so a future person doesn't "fix" it and reintroduce a diff.

### 5. `#1872` — sectional map overlaps, not yet in our doc

Five sectional-map pairs (`1`/`1N`, `5`/`6`, `12`/`13`, `20`/`21`, `20`/`26`) have
significant polygon overlap, causing some segments to get joined to multiple sectional maps
/ duplicate node assignments. Partially mitigated (per the comment thread — some cases
traced to specific node pairs 33228/9015277 and 1952/9014719) but not closed out, and not
reflected in `data_issues.md`. Possibly relevant to any lingering `int__segments_with_nodes`
uniqueness weirdness if we hit it again.

### 6. District Boundary gdb layer descriptions (`#1840`) — a `lion_outputs.csv` gap, not a diff issue

#1840's body has a full `Output Feature Class Name | Description | CSCL Source Feature
Class` table for all 43 district layers (e.g. `nyad` = "Assembly Districts clipped to
shoreline" ← `AssemblyDistrict`). Our seed's `gdb / District gdb` rows currently have
**no description column at all** — just `nyad`, `nyadwi`, etc. This is low-risk, purely
descriptive content (not a diff/behavior claim) and would make the seed self-documenting
instead of requiring someone to know what `nycdta2020` or `nymcea` stand for. Good
candidate for a `description` column addition.

### 7. `#2459` / `#2278` — these two issues are trying to do exactly what `data_issues.md` and `diffs_report.csv` already do, in prose

> "an issue to document/track CSCL GDB differences between dev and prod" (#2459)
> "This will capture the cross-team work of reviewing and resolving differences... The CSCL
> ETL - Issue tracking excel file in SharePoint is how DE and GR are coordinating" (#2278)

This is almost certainly the specific pain the "documentation in a GitHub project" complaint
is about — plus a *second* copy of the same tracking effort living in a SharePoint Excel
file, a third location for the same information. Now that `data_issues.md` +
`poc_validation/build_diffs_report.py`'s `diffs_report.csv` exist and are richer (they're
generated from real comparisons, not manually transcribed), these two issues plus the
Excel file are candidates to close/redirect to point at the repo instead of continuing to
duplicate the tracking. Worth raising with whoever owns the SharePoint sheet before doing
this unilaterally — it's cross-team (GR) coordination infrastructure, not just ours.

### 8. `#2184` — three specific, still-unresolved field-level discrepancies

A small table of 3 dev/prod mismatches (shoreline SAF at segment `0241972`, protosegment
SAF and traffic-direction diffs at specific segment/boro/facecode combos), each with
Finn's own investigation notes in the comments concluding "will flag with GR/GSS" — i.e.
already identified as **not our bug**, pending a GR response. Not obviously present in
`docs/prod_bugs/`; worth a quick check on whether these are the same three records already
covered by [Bug 007](./docs/prod_bugs/007-sept-2026-remaining-diffs-investigation.md) items
2/3 or are three *additional* ones (the segment IDs didn't obviously match on a quick
read — needs a side-by-side check, not assumed either way).

## What's *not* worth migrating

The architecture/process issues (`#2198` Architecture, `#2110` Working GDB extraction,
`#1841` ETL UI/Ergonomics, `#1842` GeoSupport Integration Tests, `#2045` gdb downstream
requirements, `#2192` Potential transformation enhancements) are legitimate project-planning
content — not data documentation — and belong in GitHub issues, not in a seed or
`data_issues.md`. The complaint this doc responds to is about *data documentation* escaping
into prose form outside the codebase, not about project management existing at all.

## Suggested next steps, roughly in priority order

1. Ask JD for the `node`/`node_stname` postprocessing script (#2054) — highest leverage,
   could shortcut further `CSCL-LION-08`/`09` work considerably.
2. Implement the coincident-segment fix from #2616 (`source_table` instead of merged
   `feature_type`) — concrete, well-scoped, not GR-blocked.
3. Rewrite `CSCL-LION-07` with #1989's actual mechanism (curve geometry lost on
   geopandas/parquet read) instead of the vague "Watch, returned in 26a" framing.
4. Add a new `data_issues.md` entry (status `Accepted`) for the 2010-vs-2020 census tract
   `seglocstatus` bug (#2193) so it's not accidentally "fixed" later.
5. Add a `description` column to `lion_outputs.csv`'s District gdb rows, sourced from
   #1840's table.
6. Cross-check #2184's three specific discrepancies against `docs/prod_bugs/` — file
   whichever aren't already covered.
7. Raise #2459/#2278 (and the SharePoint Excel tracker) with the team as candidates to
   redirect to `data_issues.md`/`diffs_report.csv` — a cross-team conversation, not a
   unilateral change.
