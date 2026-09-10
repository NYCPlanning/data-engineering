# Bug 008: Special Address Flag Leaks Onto On/Off-Ramp Protosegment in Production

**Status:** Fixed in new ETL (no code change needed - already correct)
**Affected Output:** LION .dat files (Queens)
**Severity:** Low - 1 record identified so far
**Discrepancy Count:** 1 record

## Summary

For segmentid 174704 (borough 4/Queens), production prints `special_address_flag = 'B'` on
lionkey `4939600450` - a protosegment whose `ALT_SEGDATA_TYPE` is `'R'` (combined on/off-ramp).
Per the legacy ETL's own documented rule, an `R`-type ramp protosegment must never carry a SAF
flag of type A, B, C, D, E, F, O, P, S, or V, since ramps have no addresses. Our new ETL
correctly suppresses the flag here (blank); production does not.

## Technical Details

segmentid 174704 has two representations in `int__lion`:

| lionkey | source_table | face_code | alt_segdata_type | special_address_flag (ours) | special_address_flag (prod) |
|---|---|---|---|---|---|
| 4560207075 | centerline | 5602 | (n/a) | `B` | `B` |
| 4939600450 | altsegmentdata | 9396 | `R` | *(blank)* | `B` |

Both `stg__altsegmentdata_saf` and `dcp_cscl_addresspoints` carry a real `saftype = 'B'` record
for segmentid 174704, which is correctly surfaced on the centerline row (4560207075) in both our
output and prod's. The disagreement is only on the second row - the `R`-type ramp protosegment.

[design_doc.md § Special Address Flag](../../design_doc.md#special-address-flag) quotes the
legacy ETL's own documentation verbatim:

> If the proto-segment is of the combined on-off ramp type (ALT_SEGDATA_TYPE = R): It will not
> have any appurtenant SAF type A, B, C, D, E, F, O, P, S or V data, since these are all
> address-related and ramps will never have addresses.

`int__lion.sql`'s suppression logic (lines 172-178) implements exactly this:

```sql
CASE
    WHEN NOT (
        segments.source_table = 'altsegmentdata'
        AND proto.alt_segdata_type <> 'B'
        AND saf.special_address_flag IN ('A', 'B', 'C', 'D', 'E', 'F', 'O', 'P', 'S', 'V', 'G', 'N', 'X')
    ) THEN saf.special_address_flag
END AS special_address_flag,
```

For lionkey 4939600450, `source_table = 'altsegmentdata'`, `proto.alt_segdata_type = 'R'`
(`<> 'B'`), and the joined `saf.special_address_flag = 'B'` is in the suppression list, so the
`CASE` correctly nulls the field.

## Example

Query used to confirm the protosegment's `alt_segdata_type`:

```sql
SELECT il.lionkey, il.segmentid, il.boroughcode, il.face_code, p.alt_segdata_type
FROM int__lion il
LEFT JOIN stg__altsegmentdata_proto p ON il.globalid = p.globalid
WHERE il.segmentid = 174704;
```

```
  lionkey   | segmentid | boroughcode | face_code | alt_segdata_type
------------+-----------+-------------+-----------+------------------
 4560207075 |    174704 |           4 |      5602 |
 4939600450 |    174704 |           4 |      9396 | R
```

## Root Cause

Production appears to leak the SAF flag from the segment's sibling centerline record onto its
on/off-ramp protosegment, contradicting the ETL's own documented rule that ramps never carry
address-related SAF types.

## New ETL Implementation

No change required - `int__lion.sql`'s existing suppression `CASE` (lines 172-178) already
implements the documented rule correctly.

## Impact Assessment

**Affected Records:** 1 confirmed (`_lion_key` = `493960174704`). Not yet checked whether other
`ALT_SEGDATA_TYPE = 'R'` protosegments citywide have the same leak in production - see below.

**Recommendation:** Marked `accounted_for = true`, fingerprinted by `_lion_key`, in
`qa__diffs_lion_dat.sql`.

**Follow-up worth doing:** query all `ALT_SEGDATA_TYPE = 'R'` protosegments citywide against
production's LION output to see whether this leak is systemic (in which case a field-pattern
rule, not just this one `_lion_key`, may eventually be worth adding) or genuinely a one-off.

## References

- SAF/ALT_SEGDATA_TYPE spec quoted from
  [design_doc.md § Special Address Flag](../../design_doc.md#special-address-flag)
- Diff accounting: `models/etl_dev_qa/diffs/lion_dat/qa__diffs_lion_dat.sql`
- Originally surfaced in
  [Bug 007](./007-sept-2026-remaining-diffs-investigation.md#2-special_address_flag-lion_dat_queens-493960174704)
