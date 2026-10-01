# Documentation Reconciliation Log

Findings from comparing generated documentation (`doc_config.yml` + model yml
descriptions) against the legacy ETL doc (`docs/ETL_V8_02012024.md`) - the "reconcile"
step of the doc-generation process (see [CONTRIBUTING.md](../CONTRIBUTING.md)). One
entry per finding, in the order found.

This is **not** `data_issues.md`. A finding here that turns out to be a real behavioral
discrepancy (our pipeline actually does something different from prod or the legacy
spec, not just documented differently) belongs in `data_issues.md` instead - open an
entry there and link to it from here rather than duplicating the analysis.

## DOC-001: RPL2 (`generic_segmenttype`) is an unvalidated assumption

Legacy (RPL Table 38, Note 1): the ETL tool **assumes, without validating it**, that
`generic_segmentid` is the SEGMENTID of a valid generic Centerline segment, and
therefore that its `SEGMENT_TYPE` is `'B'` or `'G'`. Our `int__rpl.sql` does the same
lookup (via `int__lion`) with no such validation. Documented in `rpl_by_field`'s
`generic_segmenttype` notes (`doc_config.yml`).

**Resolution:** documentation-only - matches legacy behavior, nothing to change.

## DOC-002: RPL8-11 NODEID fields are blank + logged as an error when a segment has no node

Legacy (RPL Table 38, Note 2): if a segment endpoint has no node, the field is
populated with blanks and reported as an error. Confirmed this is actually implemented
- `models/log_files/log__lion_segments_missing_nodes.sql` flags exactly this case.
Documented in `rpl_by_field`'s notes.

**Resolution:** documentation-only - the behavior already exists, it just wasn't
mentioned in any of our field descriptions. Added a pointer to the log model.

## DOC-003: LION L41's borough-code rule is more specific than "different boroughs"

The original draft of `segment_locational_status`'s description said "the neighboring
borough code... if left and right APs are in different boroughs" - true but
underspecified. The legacy doc (and `int__segment_locational_status.sql`) are more
precise: the output is specifically *whichever AP's borough differs from the segment's
own borough code*, not just "a neighboring code." Tightened the description to match.

**Resolution:** documentation-only - wording fix, no behavior question.

## DOC-004: OPEN — Face Code File's per-borough sort order isn't visible anywhere in the pipeline

Legacy (Face Code File section): each borough's output file must be **sorted on bytes
2-5** (the face code field). Traced the whole pipeline -
`face_code_by_field.sql` → `face_code.sql` → every `by_borough/*.sql` file - and found
no `ORDER BY` anywhere. The per-borough split itself is real (`WHERE dat_column LIKE
'1%'` etc.), just not the sort.

This might be handled at the dcpy export layer (outside this repo/my visibility), or it
might be a real gap. **Needs a decision before `face_code_by_field` can be marked
`reconciled`** in `doc_generation_plan.csv` - if it turns out to be a genuine
pipeline gap (not just an unsorted-but-still-correct output), it belongs in
`data_issues.md`, not here.

## DOC-005: SAFS3 (`face_code`) has a fallback the legacy doc's own text contradicts itself about

Legacy (SAF Type S Records, Table 34 Note 1) contains a tracked-changes edit: the
original text said no output record is written if StreetName has no entry for
`B7SC_ACTUAL`, *or the entry exists but has no Face Code value*; that second clause is
struck through in the source, and a separate marked-up addition says instead "when the
B7SC_ACTUAL doesn't have a face code, the LION key should be generated from the face
code and sequence number of the segment the address point is associated to" - i.e. a
fallback, not a hard failure.

`int__saf_s.sql` implements the *new* (fallback) rule, not the *original* (fail) one:
`COALESCE(feature_names.face_code, SUBSTRING(saf.segment_lionkey, 2, 4)) AS face_code`.
Documented in `saf_s_generic_by_field`'s `face_code` column, pointing back here.

**Resolution:** documentation-only - current behavior matches the legacy doc's later,
corrected instruction. Worth knowing the earlier text (still visible, struck through,
in `ETL_V8_02012024.md`) describes the *opposite* behavior, in case anyone reads that
section literally without noticing the edit.
