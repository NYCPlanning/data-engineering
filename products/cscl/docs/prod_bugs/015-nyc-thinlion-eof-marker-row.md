# Bug 015: `nyc.thinlion`'s Trailing DOS EOF Marker Read as a Spurious Data Row

**Status:** Fixed
**Affected Output:** `thinlion_all` comparison only (`qa__diffs_thinlion_summary`)
**Severity:** Low - 1 spurious row, comparison-tooling artifact, not a real output discrepancy
**Discrepancy Count:** 1 row (`only_in_legacy`) before the fix; 0 after

## Summary

`qa__diffs_thinlion_summary` reported one `thinlion_all` row as `only_in_legacy` with a garbage
synthetic key (`comparison_id = '\x1A000000'`) and every real field blank. This wasn't a real
ThinLION record prod published - it was prod's real delivered `nyc.thinlion` file ending in a
trailing DOS-era Ctrl-Z (`0x1A`) EOF marker byte with no newline after it, which
`poc_validation/prod_data_loader.py`'s `parse_file` (plain `for row in f:` line iteration) was
reading as one more data row.

## Technical Details

Confirmed at the byte level - `nyc.thinlion`'s last bytes are `0d 0a 1a` (`\r\n` ending the real
last record, then a lone `0x1A` with no trailing newline):

```
$ xxd nyc.thinlion | tail -1
00851b30: 0d0a 1a                                  ...
$ python3 -c "print(len(list(open('nyc.thinlion'))))"
69787   # one more than the real record count
```

`parse_file` slices each "row" by fixed field positions regardless of content, so this
single-byte line produced a record with `borough = '\x1a'` (the byte sliced into field 1) and
every other field blank/out-of-range - which is exactly the synthetic key
`qa_int__prod_thinlion_all.sql` builds from `borough || censustract_2020_basic || ... ||
dynamic_block`.

**Swept every other flat file in the 26c delivery for the same tail pattern - only
`nyc.thinlion` has it.** No fix needed elsewhere; `parse_file` is shared but the fix is a no-op
for files that don't end this way.

## Root Cause

Legacy DOS-era file-generation convention (Ctrl-Z as an EOF sentinel byte) on GR's side, read
naively by our own line-based parser - a comparison-tooling gap, not a real output difference.
Matches the project's recurring lesson: check the comparison methodology before assuming the
data is wrong.

## New ETL Implementation

`parse_file` now skips any line that's empty once stripped of whitespace and the `0x1A` marker,
before the fixed-width field slicing:

```python
if row.strip("\x1a\r\n ") == "":
    continue
```

## Impact Assessment

**Affected Records:** 1 spurious row, `thinlion_all` only. Verified: reparsing `nyc.thinlion`
with the fix yields 69,786 rows (one fewer, matching the real record count) with no blank/garbage
rows, and the last row is real data (a valid Staten Island record). The stale row already loaded
in `production_outputs.thinlion_all` was deleted directly rather than requiring a full reload.

## References

- Fix: `poc_validation/prod_data_loader.py`'s `parse_file`
- Key construction: `models/qa_int/thinlion/qa_int__prod_thinlion_all.sql`
- Diff surfaced via: `models/etl_dev_qa/diffs/thinlion/qa__diffs_thinlion_summary.sql`
