# Bug 009: `thined.txt` District Fields Diverge from Prod, Likely Source-Vintage Mismatch

**Status:** Under Investigation - blocked, root cause unconfirmed
**Affected Output:** `thined.txt` (Thin Election District flat file, `file_id: thined_dat`)
**Severity:** Medium - ~2% of rows per affected field, no fix identified yet
**Discrepancy Count:** 4267 rows in common by the real key (see below); of those,
`congress_district` differs on 61, `state_sen_district` on 69, `muni_court_district` on 65,
`city_council_district` on 88. Plus a row-count gap: 18 (assembly_district, election_district)
pairs exist only in dev, 74 only in prod (net -56).

## Summary

`thined.txt` maps each Election District to its overlapping Assembly, Congressional, State
Senate, Municipal Court, City Council district, and Borough. Comparing 26c dev output against
prod's real `thined.txt` (`edm-private/cscl_etl/26c/thined.txt`) shows real, substantial
mismatches in exactly the four fields tied to offices that went through redistricting since the
last cycle (Congressional/State Senate post-2020-census, NYC Council 2023) - `assembly_district`
and `borough` are essentially untouched (0 and 1 mismatches respectively).

The model itself (`thined_by_field_unformatted.sql`) does no transformation on these four
fields - it selects them straight off `stg__electiondistrict`'s own attributes. So this looks
like a genuine difference in the `ElectionDistrict` source layer's baked-in district-code
attributes between whatever vintage our 26c archive holds and whatever prod's separate legacy
pipeline saw when it generated its own `thined.txt` - not a bug in this pipeline's logic. That's
a hypothesis, not confirmed; see Root Cause.

**No `qa__` dbt model exists for this file** - grepping every model for "thined" turns up only
`thined_by_field.sql`/`thined_dat.sql` themselves. The only signal is
`poc_validation/validate_outputs.sh`'s blunt line-level diff (`comm -23` on sorted whole lines),
which just reports a flat mismatched-row count with zero field-level attribution and no
attribution of which field(s) actually differ.

## Technical Details

**A naive read of the raw diff is actively misleading.** `election_district` alone is only 3
bytes and is numbered *within* each Assembly District, not citywide - it repeats roughly 40x
across the file (109-114 distinct 3-digit values across ~4300+ rows). The model's own comment
confirms the real identity:

```sql
-- Derive borough from atomicpolygons by finding any AP with matching ED+AD
-- The combination of Election District + Assembly District is unique citywide
```
(`models/product/thined/thined_by_field_unformatted.sql`)

Keying on `election_district` alone (as a first pass at this investigation did) collapses 4285
dev rows down to 109 "unique" values and produces meaningless per-column diff counts. Keying on
`assembly_district + election_district` instead gives **zero duplicate keys on either side** -
confirmed empirically against both files - and is the key this investigation's numbers above are
based on.

**Field breakdown** (source layer: `models/product/thined/thined_by_field_unformatted.sql`, a
direct pass-through of `stg__electiondistrict`'s `congress_district`, `state_sen_district`,
`muni_court_district`, `city_council_district` columns, no join or spatial computation):

| field | mismatches (of 4267 common rows) |
|---|---|
| assembly_district | 0 (part of key) |
| borough | 1 |
| congress_district | 61 |
| state_sen_district | 69 |
| muni_court_district | 65 |
| city_council_district | 88 |

### Reproducing this

```python
widths = [(0, 3), (3, 5), (5, 7), (7, 9), (9, 11), (11, 13), (13, 14)]
names = [
    "election_district",
    "assembly_district",
    "congress_district",
    "state_sen_district",
    "muni_court_district",
    "city_council_district",
    "borough",
]
# parse both files fixed-width per seeds/text_formatting/text_formatting__thined_dat.csv,
# key on assembly_district + election_district, then diff field by field.
```

Dev file: `output/dataset_files/thined.txt` from a build. Prod file:
`edm-private/cscl_etl/<version>/thined.txt` (fetch with `dcpy.utils.s3.download_file`).

## Root Cause

**Unconfirmed.** Leading hypothesis: the `ElectionDistrict` layer inside the `ETL Working
GDB.gdb.zip` archived for this release and prod's separately-generated `thined.txt` were not
built from the same underlying snapshot - i.e. a timing/version skew in when each side's
`ElectionDistrict` attributes were last refreshed against the most recent redistricting maps.
This is consistent with the *pattern* (only the recently-redistricted offices are affected,
untouched offices match almost perfectly) but has not been verified against a changelog or
confirmed with GR.

Ruled out: a bug in our join/transform logic - there isn't one for these four fields, they're a
straight column pass-through from source.

Worth double-checking given this 26c release's source files were assembled by hand this cycle
(several were initially missing or corrupted - see recent branch history): confirm the
`ElectionDistrict` layer and `thined.txt` in whatever bundle produced this result are actually
guaranteed to come from the same snapshot, rather than assuming it because they arrived in the
same folder.

## New ETL Implementation

N/A - there is no transform to fix on our side unless the root cause turns out to be something
other than source-vintage skew. If GR confirms a source-timing mismatch, the resolution is
re-archiving a contemporaneous `ElectionDistrict` layer, not a SQL change.

## Impact Assessment

**Affected Records:** ~2% of rows per affected field (61-88 out of 4267), plus a 56-row net
count gap in (assembly_district, election_district) coverage.

**Recommendation:** Do not mark `accounted_for` yet - root cause isn't confirmed. Raise with GR:
whether `ElectionDistrict`'s district-code attributes and `thined.txt` are generated from the
same snapshot, and if not, get a contemporaneous re-archive.

## References

- `seeds/lion_outputs.csv`'s `thined_dat` row: `status=blocked`, points here.
- Model: `models/product/thined/thined_by_field_unformatted.sql`,
  `models/product/thined/thined_by_field.sql`
- Formatting spec: `seeds/text_formatting/text_formatting__thined_dat.csv`
- Diff tooling gap: no `qa__` model covers this file; only
  `poc_validation/validate_outputs.sh`'s whole-line comparison does, and it isn't attributed by
  field.
