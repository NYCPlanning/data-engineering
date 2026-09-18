# Bug 010: Prod's FEATURENAME-Derived Normalizing Tables Carry Stale Entries

**Status:** Under Investigation - blocked, needs GR input
**Affected Output:** `Exception.txt` (`exception`), `Enders.txt` (`enders`), `SND.txt` (`snd`) -
all three derived from `stg__facecode_and_featurename`
**Severity:** Low-Medium - small absolute counts on Exception/Enders (4 and 6 records), SND at
much larger scale (102 records, confirmed same cause)
**Discrepancy Count:** Exception: 4 (3 only-in-prod, 1 only-in-dev). Enders: 6 (all only-in-prod).
SND: 102 (96 only-in-prod, confirmed **100% (96/96) truly absent from current source** by a
full join, not a sample; 6 modified, 0 only-in-dev).

## Summary

Comparing dev's 26c output against prod's real files for `Exception.txt`, `Enders.txt`, and
`SND.txt` (`qa__diffs_exception`, `qa__diffs_enders`, `qa__diffs_snd` - real keyed dbt
comparisons, not the raw line diff) turns up prod-only rows whose text **does not correspond to
any record in the current `stg__facecode_and_featurename` source at all** - not even as a
near-miss with different flags, confirmed by a full join for all 96 of SND's affected rows, not
a sample. All three models are simple, unconditional filters over that one source table
(`enders_flag = 'Y'` for Enders, `exception_flag = 'Y' AND enders_flag = 'N'` for Exception,
similar for SND - see each model's SQL) - there is no dedup or transform on our side that could
be silently dropping real current-source rows. If the source doesn't have it, dev can't produce
it. SND additionally has 6 `modified` rows where the key still exists but its content changed -
two of them (`b10sc` 20148701010/20148701020) show prod's `full_name` still reading `ISOBEL
ROONEY MS 80`/`ISOBEL ROONEY M S 80`, now `ST GABRIEL CHURCH`/`SAINT GABRIEL CHURCH` in current
source - **the same feature Exception.txt's only-in-legacy row references**, directly confirming
Exception and SND's anomalies share one real-world cause, not independent coincidences.

This looks like the same shape of problem as the LDF's cumulative record numbering and
`thined.txt`'s stale AD/ED mappings (see
[Bug 009](./009-thined-redistricted-field-mismatch.md)): **prod's legacy pipeline doesn't appear
to fully regenerate these tables from the current release's source each cycle** - some entries
persist in prod's output after the underlying FEATURENAME/StreetName record they came from has
been edited, renamed, or removed from CSCL. Unconfirmed; needs GR input on how these are
actually built.

## Technical Details

### Exception.txt (`qa__diffs_exception`, 4 anomalies)

| key | status |
|---|---|
| `MANHATTAN H S FOR ` | only_in_legacy |
| `SPELLMAN H S ` | only_in_legacy |
| `ISOBEL ROONEY M S ` | only_in_legacy |
| `E ` | only_in_build |

None of the three `only_in_legacy` strings match any row in `stg__facecode_and_featurename`,
even loosely (`ILIKE '%MANHATTAN%H S%'`, `ILIKE '%SPELLMAN%'`, `ILIKE '%ISOBEL ROONEY%'` all
return zero exact matches, only unrelated near-miss school names). The one `only_in_build` row,
`E`, is legitimate - it exists twice in current source, both matching the exact filter
(`exception_flag = 'Y' AND enders_flag = 'N'`) `int__exception.sql` implements.

### Enders.txt (`qa__diffs_enders`, 6 anomalies, all only-in-prod)

```
STEINER H SOUTH                  RUDOLF STEINER H SOUTH           AARON M SOUTH
SPELLMAN H SOUTH                 HARLEM RENAISSANCE H SOUTH       AARON H SOUTH
```

Same pattern: none of these exact strings exist in current source. Related-but-different rows
do (`RUDOLF STEINER HS`, `RUDOLF STEINER H S`, `RUDOLF STEINER SCH`, `HARLEM RENAISSANCE` - all
with `enders_flag` NULL, not `'Y'`), suggesting the "H SOUTH"/"M SOUTH"-suffixed variant used to
exist with `enders_flag = 'Y'` and was since edited or replaced by the unsuffixed forms, which
prod's Enders.txt hasn't caught up to.

### SND.txt (`qa__diffs_snd`, 102 anomalies - fully confirmed)

`_snd_key = b10sc` (an 11-char borough+B7SC street code) is a clean, near-unique key on both
sides (dev: 121,069 rows / 121,067 distinct; prod: 121,165 / 121,163 distinct - 2 collisions
each, not a LegacyID-style false-positive-key situation).

**96 `only_in_legacy` rows, all confirmed genuinely absent from current source** - not a sample,
a full check:

```sql
WITH missing AS (
    SELECT comparison_id AS b10sc FROM qa__diffs_snd
    WHERE accounted_for = false AND status = 'only_in_legacy'
)
SELECT count(*), count(src.b10sc)  -- 96, 0
FROM missing
LEFT JOIN stg__facecode_and_featurename AS src ON src.b10sc = missing.b10sc;
```

All 96 prod-only `b10sc` codes have zero match in current source - not excluded by
`int__snd.sql`'s `enders_flag`/`lookup_key` filters (`int__snd.sql` reads straight from
`stg__facecode_and_featurename` with `WHERE lookup_key IS NOT NULL AND enders_flag IS DISTINCT
FROM 'Y' AND b10sc IS NOT NULL` - no dedup that could be dropping a real row). The code itself
doesn't exist in the current release's source at all.

**6 `modified` rows** - the `b10sc` still exists on both sides, but its content changed. Two of
these are the smoking gun tying this directly to Exception.txt's finding above:

| b10sc | field | prod (old) | dev (new) |
|---|---|---|---|
| 20148701010 | `full_name`/`place_name` | `ISOBEL ROONEY MS 80` | `ST GABRIEL CHURCH` |
| 20148701020 | `full_name`/`place_name` | `ISOBEL ROONEY M S 80` | `SAINT GABRIEL CHURCH` |

**This is the same "Isobel Rooney" feature Exception.txt's only-in-legacy row referenced** -
direct, concrete confirmation that prod's SND and Exception tables are both reflecting a
FEATURENAME record (a school, apparently since replaced by a church at the same location or
renamed) that the current CSCL release no longer has under that name. Not a coincidence or a
separate issue - the same real-world edit, visible in two different derived outputs. The other
4 `modified` rows are smaller attribute drifts (`geographic_feature_type`, `primary_flag`,
`principal_flag`) at different `b10sc`s, consistent with the same "prod hasn't caught up to a
source edit" story but not individually traced.

**0 `only_in_build`** for SND - unlike Exception's one legitimate dev-only row, every SND
anomaly runs in the "prod has something dev doesn't/has wrong" direction, none the reverse.

## Root Cause

**Mechanism confirmed (prod is stale relative to current source); why prod is stale is not.**
The Isobel Rooney → St Gabriel Church example removes the ambiguity the Exception-only evidence
had: this is not a coincidence of two unrelated small anomalies, and it isn't a filter/dedup bug
on our side (ruled out directly - `int__exception.sql`, `enders_by_field.sql`, and `int__snd.sql`
are all simple, unconditional flag filters over `stg__facecode_and_featurename` with no join or
dedup that could drop a real current-source row, and the SND-only-in-legacy set was confirmed
100% absent from that source by a full join, not inferred). Prod's Exception/Enders/SND tables
are carrying at least one real FEATURENAME record (and very likely all ~106 anomalies across the
three files) from a prior state of the source that has since been edited, renamed, or deleted -
this pipeline, which computes all three fresh from the current release every time, structurally
cannot reproduce an entry that no longer exists in that release's source. What's still unknown is
*why* prod hasn't picked up the change - a batch/refresh cadence slower than CSCL's own edit
cadence, a separate legacy process that isn't rerun every release, or something else - that
needs GR.

## New ETL Implementation

N/A - there's no transform to add on our side unless GR confirms a different mechanism (e.g. a
retention/carry-forward rule we're not aware of that's actually intentional, not staleness).

## Impact Assessment

**Affected Records:** Small in absolute terms for Exception (4) and Enders (6); SND at real
scale (102, ~0.08% of ~121K rows). None of these fields feed geometry or addressing logic - low
risk to the product even if unresolved for a while.

**Recommendation:** Ask GR how Exception.txt/Enders.txt/SND.txt are actually generated - fresh
each cycle from FEATURENAME/StreetName, or with some carry-forward/accretion step we're not
replicating - and specifically flag the Isobel Rooney / St Gabriel Church example as concrete
evidence to point to. If GR confirms staleness on their end, no fix needed on ours; if there's a
real retention rule, this pipeline needs to implement it (a change well beyond a QA fingerprint -
it would only be reconstructable if a source table exists somewhere in CSCL that tracks
superseded records, which isn't the same as this one currently does).

## References

- `seeds/lion_outputs.csv` rows: `exception`, `enders`, and `snd`, all `status=blocked`, point
  here.
- Models: `models/intermediate/snd_and_normalizing_tables/int__exception.sql`,
  `models/product/snd_and_normalizing_tables/enders_by_field.sql`,
  `models/intermediate/snd_and_normalizing_tables/int__snd.sql`
- QA models: `models/etl_dev_qa/diffs/normalizing_tables/qa__diffs_exception.sql`,
  `models/etl_dev_qa/diffs/normalizing_tables/qa__diffs_enders.sql`,
  `models/etl_dev_qa/diffs/snd/qa__diffs_snd.sql`
- Related: [Bug 009](./009-thined-redistricted-field-mismatch.md) (same "prod doesn't fully
  regenerate" shape, different table)
