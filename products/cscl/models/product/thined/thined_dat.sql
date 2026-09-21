WITH data AS (
    {{ select_rows_as_text(model='thined_by_field', exclude=['globalid']) }}
),

-- Prod's real thined.txt carries a 3-line, 14-byte-wide file header before
-- the data records - not documented in design_doc.md/ETL_V8_02012024.md, and
-- not produced by the legacy ETL tool archived in cscl_etl_archive (which
-- extracts only ThinLION/ThinFire as "district equivalency files" per
-- etl_docs.MD - ThinED has no source there at all, so its header's real
-- generation rule is unknown to us). Record 0002's count field is
-- self-referential - it counts every line in the file, header included -
-- confirmed by the same "record count includes header record" convention
-- design_doc.md documents explicitly for the LDF header (LDFH6); computed
-- here, not guessed. Records 0000/0001's "THIN260309"/"26A1" payloads have
-- no available source explaining what, if anything, should change about
-- them release to release - carried forward verbatim from 26c's real prod
-- file rather than guessed at. See CSCL-THINED-02 in data_issues.md before
-- assuming these are still correct on a future release.
header AS (
    SELECT '0000THIN260309' AS dat_column
    UNION ALL
    SELECT '000126A1      '
    UNION ALL
    SELECT '0002' || lpad((count(*) + 3)::text, 8, '0') || '  '
    FROM data
)

SELECT dat_column FROM header
UNION ALL
SELECT dat_column FROM data
