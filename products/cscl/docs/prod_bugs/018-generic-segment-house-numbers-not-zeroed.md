# Bug 018: Generic-Segment House Numbers Not Consistently Zeroed in Prod

**Status:** Root cause found (the real legacy tool and rule), not GR-confirmed - do not mark `accounted_for`
**Affected Output:** `gdb_lion` (the `lion` LION gdb layer) - `LLo_Hyphen`, `LHi_Hyphen`, `RLo_Hyphen`,
`RHi_Hyphen`, `FromLeft`, `FromRight`, `ToLeft`, `ToRight`
**Severity:** Low-medium - 5,928 prod rows affected (flagged by `qa__diffs_lion_house_numbers`)
**Discrepancy Count:** 5,928 rows where prod's own delivered `fgdb_lion` violates its own
documented house-number zero-out rule

## Summary

ETL spec §2.7.3 says that for certain segment types - **Generic segments
(`SegmentTyp = 'G'`)** foremost among them - all eight house-number fields
(`LLo_Hyphen`/`LHi_Hyphen`/`RLo_Hyphen`/`RHi_Hyphen` and their normalized twins
`FromLeft`/`FromRight`/`ToLeft`/`ToRight`) "will be populated with zeros, regardless of the
values in the CSCL source data." Checking prod's real, delivered `fgdb_lion` directly: **5,743
of 13,592 Generic segments (42%)** have real, nonzero values in these fields instead of zeros.

## Technical Details

The actual rule - found in the real legacy tool, not just the written spec - is both more
precise and unconditional. `cscl_etl_archive/lion_dist_tool_files/
BytesLION_CSCL_workflow_tool_PUB.py` (an ArcGIS 9.x/10.x-era Python script using the classic
`gp` Geoprocessor object, not `arcpy`) contains:

```python
#Remove House# info from Generic segment types (for geocoding purposes--to avoid duplicate matches)
gp.MakeFeatureLayer(os.path.join(r'C:\temp', Version, r'GIS_OUTPUT\lion\fgdb\lion.gdb\LION'),
                     'LION_Addressable',
                     "SegmentTyp IN ('G','F') or FeatureTyp NOT IN ( '0', '6', 'W')")
gp.CalculateField('LION_Addressable', 'LLo_Hyphen', '""')
gp.CalculateField('LION_Addressable', 'LHi_Hyphen', '""')
gp.CalculateField('LION_Addressable', 'RLo_Hyphen', '""')
gp.CalculateField('LION_Addressable', 'RHi_Hyphen', '""')
gp.CalculateField('LION_Addressable', 'FromLeft', 0)
gp.CalculateField('LION_Addressable', 'FromRight', 0)
gp.CalculateField('LION_Addressable', 'ToLeft', 0)
gp.CalculateField('LION_Addressable', 'ToRight', 0)
```

Three things this reveals that the written spec alone doesn't:

1. **The selection is `SegmentTyp IN ('G','F') OR FeatureTyp NOT IN ('0','6','W')`** - note
   `'F'` alongside `'G'`, which the ETL spec text we'd been working from never mentions.
2. **It's unconditional.** Every row matching the selection gets all eight fields zeroed - no
   carve-out, no exception for some subset of Generic segments. So a 42%-compliant result isn't
   "the rule has more nuance than we knew"; it's prod not consistently applying a rule that is,
   in its own terms, absolute.
3. **The stated motivation is about geocoding, not addressing.** The comment says this exists
   "for geocoding purposes--to avoid duplicate matches" - it's a compatibility patch for the
   address locator built a few steps earlier in the same script
   (`gp.StandardizeAddresses(...)`), not a structural truth about what addresses Generic
   segments do or don't have.

Most tellingly: this script **mutates the real, final `LION` feature class directly**
(`GIS_OUTPUT\lion\fgdb\lion.gdb\LION` - the exact feature class
`CSCL.ETL.Extractor`'s `ESRIBytesConfiguration.xml` names as its data source for every
published output), via a hardcoded local path (`C:\temp\<Version>\...`). That's a strong signal
this is a **manually-triggered, operator-run cleanup step**, not an automated, idempotent
transformation that re-applies itself whenever the underlying data changes. If a Generic segment
enters (or re-enters, via an update) the `LION` feature class after this step last ran for a
given release, it would never get zeroed - a direct, plausible explanation for the 42%.

Searched `cscl_etl_archive` end-to-end (all 476 files, including `ExtractorClass.cs`, the one
file confirmed to hold real segment-level business logic from the `Join_ID` investigation -
CSCL-LION-09) for any trace of this rule being applied anywhere else, automated or not: nothing.
Whatever decides this, decides it only in this one manually-run script.

## QA Implementation

`models/qa_int/qa_int__prod_fgdb_lion.sql`: a thin QA-only projection of
`production_outputs.fgdb_lion`, scoped to the fields needed for this investigation, keyed on
`SegmentID|Join_ID` (rtrim'd - see CSCL-LION-10 in data_issues.md for why).

`models/etl_dev_qa/diffs/lion_gdb/qa__diffs_lion_house_numbers.sql`'s **row selection** is not a
dev-vs-prod diff, despite sharing the `qa__diffs_*` schema: our own `gdb_lion.sql` has never
implemented `FromLeft`/`FromRight`/`ToLeft`/`ToRight` at all (still hardcoded `NULL::int`,
tracked separately in `compare_gdb.py`'s `KNOWN_NULL_COLUMNS`), so gating which rows get flagged
on a dev/prod difference would flag nearly every row with any address at all, Generic or not -
tried first, produced over 200,000 spurious rows of pure noise. What's actually checkable, and
what decides which rows are flagged, is whether **prod's own data is internally consistent with
its own documented rule** - rows matching the GP tool's selection
(`SegmentTyp IN ('G','F') OR FeatureTyp NOT IN ('0','6','W')`) that don't match the
all-blank/all-zero outcome that rule demands. The **`changes` payload itself does follow the
normal `old`=prod/`new`=dev convention** - for each violating field, `old` is prod's real (buggy)
value and `new` is our dev build's actual value for that same field (plain absent/null for
`FromLeft`/`FromRight`/`ToLeft`/`ToRight`, since those are genuinely unimplemented on our side -
`jsonb_strip_nulls` drops a key entirely when its value is null, so an omitted `new` there means
exactly that). Dev's values are joined in for display only, via `gdb_lion` directly (not a
separate `_by_field` model - not needed once this stopped being a real row-paired diff); see the
model's header comment for the ~0.7% duplicate-key handling (CSCL-LION-10).

**5,928 rows flagged, all `accounted_for = FALSE`.** Per explicit instruction: do not mark these
accounted for. The manual/inconsistently-applied-script hypothesis is strong and well-evidenced,
but not GR-confirmed - someone with visibility into how/whether this script is actually run each
release needs to weigh in first.

## New ETL Implementation (pseudocode only - not implemented)

See the comment block directly above the `"LLo_Hyphen"`/`"FromLeft"` etc. fields in
`models/product/lion/gdb/gdb_lion.sql` for the sketch of how this would be implemented, if/when
confirmed. Not implemented in SQL - this is confirmed, real, authoritative legacy logic (unlike
most "stale prod" findings this cycle), so if GR confirms the rule should hold, implementing it
is a straightforward CASE/zero-out, not a judgment call. Left as pseudocode pending that
confirmation.

## Impact Assessment

**Affected Records:** 5,928 rows in prod's real `fgdb_lion`, all Generic/`'F'`-type or
non-physical-`FeatureTyp` segments. Not yet assessed whether implementing this rule on our side
(to match prod's *documented*, not actual, behavior) would help or hurt the broader `lion` gdb
layer comparison, given prod's own inconsistent application - see "What would settle it."

## References

- Spec: `docs/ETL_V8_02012024.md`, ETL spec §2.7.3 (BL101-104, ~line 4847-4910)
- Real legacy logic: `cscl_etl_archive/lion_dist_tool_files/BytesLION_CSCL_workflow_tool_PUB.py`
  (lines ~352-361)
- QA: `models/qa_int/qa_int__prod_fgdb_lion.sql`,
  `models/etl_dev_qa/diffs/lion_gdb/qa__diffs_lion_house_numbers.sql`
- Pseudocode: `models/product/lion/gdb/gdb_lion.sql`, comment above the house-number fields
- Related: `compare_gdb.py`'s `KNOWN_NULL_COLUMNS["lion"]` (FromLeft/ToLeft/FromRight/ToRight
  still unimplemented), CSCL-LION-10 in `data_issues.md` (same layer's key/Join_ID padding
  issues)

## What would settle it

Ask GR/the legacy pipeline owner: is `BytesLION_CSCL_workflow_tool_PUB.py`'s house-number
zero-out step run every release, on a fresh copy of the `LION` feature class, or can segments
enter/re-enter the feature class after it's already run? If the latter, that's the mechanism,
confirmed - and worth knowing whether it's considered a bug worth fixing on their side, or an
accepted quirk of a geocoding-compatibility patch that was never meant to be perfectly applied.
