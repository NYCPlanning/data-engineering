# Bug 014: ThinFire Borough Assignment Used an Arbitrary AtomicPolygon Pick Instead of a Spatial Match

**Status:** Fixed
**Affected Output:** `ThinFire` (`BronxThinFire.txt`, `BrooklynThinFire.txt`, `ManhattanThinFire.txt`,
`QueensThinFire.txt`, `StatenIslandThinFire.txt`)
**Severity:** Medium - 12 fire companies citywide, each appearing in the wrong borough's file
**Discrepancy Count:** 24 rows (12 companies × dev-only + prod-only) across 4 of the 5 borough
files before the fix; 0 after

## Summary

`thinfire_by_field_unformatted.sql` assigned each fire company's `Borough` field (TF5) by
picking an arbitrary AtomicPolygon tagged with that company's code and using its `borocode` -
`ORDER BY ap.atomicid LIMIT 1`, i.e. whichever matching AtomicPolygon happened to have the
lowest ID. For any company whose territory spans multiple AtomicPolygons in more than one
borough (rivers, bridges, boundary streets - completely normal for a fire company's district),
this is a coin flip with no geographic meaning: our arbitrary pick and prod's independent
arbitrary pick have no reason to agree.

12 companies were affected, and the mismatch pattern was strikingly one-directional: 10 of the
12 companies' arbitrary pick landed in Manhattan (E068, L052, E202, E205, E221, E224, L106,
L118, E262, L115; the other 2 - E266, L173 - landed in Brooklyn) regardless of prod's actual
(correct) borough. This wasn't a Manhattan-specific bug - it's consistent with Manhattan's
AtomicPolygons simply having lower `atomicid` values than the other boroughs', so any company
touching Manhattan at all, even via a single boundary-adjacent AtomicPolygon, was biased toward
being picked there.

## Technical Details

### The spec explicitly authorizes an arbitrary pick

`docs/ETL_V8_02012024.md`, Table 25 Note 1 (the TF5/Borough field):

> The FireCompany feature class does not have an attribute for Borough. Although it would be
> possible to determine the borough spatially by forming a fire company centroid and performing
> a point-in-polygon match to borough polygons or AP polygons, the following alternative method
> involving attribute matching appears likely to provide better performance efficiency and is
> recommended for consideration by the developer: **Populate TF5 with the BOROUGH attribute of
> any of the matching AP's.**

So this was never a deviation from spec - the spec itself says "any." It explicitly considered
the spatial approach and rejected it for ESRI-desktop-era performance reasons that don't apply
to Postgres/PostGIS. The three already-hardcoded special cases (`E-81`, `E-260`, `E-263`) are
the spec's own acknowledgment that "any" can be wrong; it just didn't anticipate these
additional 12.

### Verifying the fix before applying it

Rather than guess at a tiebreak rule, three candidates were tested against real data - the 12
known-mismatched companies' actual prod-observed borough (extracted from the frozen prod
ThinFire files - see the "stale prod data" note below) plus the 3 spec-documented special cases:

| Method | Result on 12 known mismatches | Result on 3 special cases (E81/E260/E263) |
|---|---|---|
| AtomicPolygon count-majority (`GROUP BY borocode ORDER BY count(*) DESC`) | 12/12 correct | 3/3 correct |
| AtomicPolygon area-majority (`GROUP BY borocode ORDER BY sum(ST_Area(geom)) DESC`) | 12/12 correct | 3/3 correct |
| Company-centroid point-in-polygon vs. `stg__borough` (the spec's own "proper" method, same pattern as `docs/prod_bugs/002-police-geo-centroid-mismatch.md`) | 12/12 correct | 3/3 correct |

All three methods agreed on every test case - not surprising, since a company's true "home"
borough should dominate by count, by area, and contain its centroid simultaneously in the
typical case. The spatial centroid method was chosen for consistency with the existing police
precinct/sector/patrol-borough join (`thinlion_by_field_unformatted.sql`), which already
established this exact pattern (centroid, falling back to `ST_PointOnSurface` if the centroid
lands outside the polygon) for the same class of problem.

**Full-roster regression check:** ran the spatial method against every fire company (not just
the 12 known cases) and diffed against the old method's output - exactly those 12 companies
changed, zero others. The 3 hardcoded special cases were also tested and don't need the
override under the spatial method (it gets them right on its own), but the hardcoding was kept
anyway as an explicit backstop matching the spec's own documented exceptions, in case source
geometry ever shifts.

### The fix

`models/product/thinfire/thinfire_by_field_unformatted.sql`'s `ELSE` branch now does:

```sql
ELSE (
    SELECT b.borocode
    FROM {{ ref('stg__borough') }} AS b
    WHERE ST_WITHIN(
        CASE
            WHEN ST_WITHIN(ST_CENTROID(fc.geom), fc.geom) THEN ST_CENTROID(fc.geom)
            WHEN ST_POINTONSURFACE(fc.geom) IS NOT NULL THEN ST_POINTONSURFACE(fc.geom)
            ELSE ST_CENTROID(fc.geom)
        END,
        b.geom
    )
)
```

instead of the AtomicPolygon `ORDER BY atomicid LIMIT 1` lookup.

### A second, independent finding: prod's ThinFire hasn't changed since 26b

While investigating, `production_outputs.thinfire_<borough>` (loaded and recorded as "26c") was
compared against the `production_outputs_26b` snapshot: **byte-for-byte identical content hash
on all 5 boroughs.** GR's ThinFire delivery is the same file re-shipped each cycle, not
regenerated - the same "prod doesn't fully regenerate this each cycle" pattern already
documented in Bug 010 (normalizing tables) and Bug 013 (frozen district gdb layers), now
confirmed for a fourth output family. This doesn't affect the fix above (which was verified
against prod's actual, if stale, values), but it means ThinFire diffs should be re-checked
whenever prod's ThinFire delivery is eventually refreshed.

## Root Cause

The legacy ETL spec explicitly authorizes an arbitrary AtomicPolygon pick for performance
reasons that don't apply to a Postgres/PostGIS implementation. Reimplementing the "proper"
spatial method the spec itself describes (and already used elsewhere in this pipeline for the
same class of jurisdiction-assignment problem) resolves it.

## New ETL Implementation

Company-centroid (with `ST_PointOnSurface` fallback) point-in-polygon match against
`stg__borough`, matching the pattern in `thinlion_by_field_unformatted.sql`'s police
precinct/sector/patrol-borough joins. The 3 spec-documented special cases (`E-81`, `E-260`,
`E-263`) remain hardcoded as an explicit backstop, though the spatial method resolves them
correctly on its own.

## Impact Assessment

**Affected Records:** 12 fire companies, 24 rows across 4 borough files before the fix.

**Verified:** `qa__diffs_thinfire_{bronx,brooklyn,manhattan,queens,statenisland}` all return 0
rows after the fix (previously 2/8/10/4/0 unaccounted respectively). Full company-roster
regression check confirms zero other companies affected.

## References

- Spec: `docs/ETL_V8_02012024.md`, Table 25 Note 1 (TF5/Borough)
- Fix: `models/product/thinfire/thinfire_by_field_unformatted.sql`
- Precedent pattern: `docs/prod_bugs/002-police-geo-centroid-mismatch.md`,
  `models/product/thinlion/thinlion_by_field_unformatted.sql:131-159`
- Stale-prod-data precedent: `docs/prod_bugs/010-featurename-normalizing-tables-stale-accretion.md`,
  `docs/prod_bugs/013-gdb-creadate-staleness-fossils.md`
