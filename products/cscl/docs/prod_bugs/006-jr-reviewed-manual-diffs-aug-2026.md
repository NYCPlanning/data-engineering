# Bug 006: Diffs Individually Reviewed and Confirmed Correct by JR (August 2026)

**Status:** JR-confirmed correct via manual spreadsheet review - not recreating prod's values
**Affected Output:** LION .dat files (Manhattan, Bronx, Brooklyn, Queens), SAF `s_generic`/`s_roadbed`
**Severity:** Low - 12 records
**Discrepancy Count:** 12 records across 6 fields/field-groups (see table below)

## Summary

JR reviewed this specific batch of 12 discrepant records against source data in a spreadsheet
he sent over in August 2026, and confirmed that our (new) values are correct in each case.
Unlike Bugs 001-005, this is not one systematic root cause - it's a curated set of
individually-eyeballed records spanning several unrelated fields. Each is fingerprinted by its
exact `comparison_id` (`_lion_key`/`_saf_key`) rather than by field-change pattern, so this does
**not** blanket-approve every diff on the same field - e.g. `coincident_seg_count` diffs outside
this list remain unaccounted for and open (see `CSCL-LION-06` in `data_issues.md`).

| comparison_id | table | field(s) changed | old &rarr; new |
|---|---|---|---|
| 101650330641 | lion_dat_manhattan | `left_dynamic_block`, `right_dynamic_block`, `left_2020_census_block_basic`, `right_2020_census_block_basic` | swapped left/right values |
| 116000297614 | lion_dat_manhattan | `coincident_seg_count` | 0 &rarr; 1 |
| 125250032941 | lion_dat_manhattan | `coincident_seg_count` | 0 &rarr; 1 |
| 130400327255 | lion_dat_manhattan | `coincident_seg_count` | 0 &rarr; 1 |
| 236590241972 | lion_dat_bronx | `special_address_flag` | ` ` &rarr; `N` |
| 333280149383 | lion_dat_brooklyn | `coincident_seg_count` | 2 &rarr; 1 |
| 333280149678 | lion_dat_brooklyn | `coincident_seg_count` | 2 &rarr; 1 |
| 335640029283 | lion_dat_brooklyn | `coincident_seg_count` | 2 &rarr; 1 |
| 335640188023 | lion_dat_brooklyn | `coincident_seg_count` | 2 &rarr; 1 |
| 416140101502 | lion_dat_queens | `curve_flag` | `R` &rarr; `L` |
| 416070284412 | saf_s_roadbed | `lgc1` | `01` &rarr; `10` |
| 416070284412 | saf_s_generic | `lgc1` | `01` &rarr; `10` |

(A 13th record from the reviewed batch, `107660240593` / `saf_abcegnpx_generic` /
`side_ap` `118` &rarr; `901`, was already `accounted_for = true` under
[Bug 003](./003-saf-gnx-side-ap-point-mismatch.md) before this review, so it needed no change.)

## Technical Details

No per-field root cause is documented here - that's the point of this bug entry vs. 001-005.
JR checked each record against source data directly (not via a documented mechanism) and
confirmed our new value; the reasoning behind why prod differs was not captured beyond that.
If a broader pattern is later identified for one of these fields (e.g. `coincident_seg_count`
or `curve_flag` more generally), it should get its own numbered bug with a real technical
explanation, and these `comparison_id`s can be folded into it.

## Root Cause

Not investigated per-field. Confirmed by manual review only (JR, August 2026, via a
spreadsheet emailed to the team).

## New ETL Implementation

No code change - these are QA-tooling classification changes only (`accounted_for`), not
pipeline logic changes.

## Impact Assessment

**Affected Records:** 12 records total - 10 in `qa__diffs_lion_dat.sql`, 1 in
`qa__diffs_saf_s_roadbed.sql`, 1 in `qa__diffs_saf_s_generic.sql` (same `_saf_key`, two
different output files).

**Recommendation:** Marked as `accounted_for = true`, fingerprinted by exact `comparison_id`,
in `qa__diffs_lion_dat.sql`, `qa__diffs_saf_s_roadbed.sql`, and `qa__diffs_saf_s_generic.sql`.

## References

- Partially overlaps `CSCL-LION-06` (coincident segments, still `Open` in general) in
  `data_issues.md` - only the specific records above are settled.
- Diff accounting: `models/etl_dev_qa/diffs/lion_dat/qa__diffs_lion_dat.sql`,
  `models/etl_dev_qa/diffs/saf/qa__diffs_saf_s_roadbed.sql`,
  `models/etl_dev_qa/diffs/saf/qa__diffs_saf_s_generic.sql`
