-- LION gdb (prod's ESRI LION) rows prod leaves out for a missing name or street code
-- (ExtractorClass.cs:2432-2471, AddEsriBytesFeature):
--   BYTES-01: a SAF replicant whose SAFStreetCode is blank.
--   BYTES-02: a SAF replicant whose street code has no principal name in the StreetName
--     or FeatureName table (SetSAFStreetNameAndCode, :4970-5215), so SAFStreetName is blank.
--   BYTES-03: a feature whose Street is blank (its B7SC has no principal name). Logged once
--     per feature; prod logs it again for each of the feature's SAF replicants.
-- gdb_lion writes all of them except G/N/X CommonPlace replicants with no name. Prod names
-- the replicated feature by its OBJECTID; we log the SAF record's GlobalID and the
-- replicant's LIONKEY, or the feature's GlobalID and SEGMENTID.
WITH lion AS (
    SELECT * FROM {{ ref('int__lion') }}
    WHERE include_in_bytes_lion
),
replicants AS (
    SELECT DISTINCT
        replicants.saf_globalid,
        replicants.saf_source_table,
        replicants.specaddr,
        replicants.lionkey,
        replicants.saf_street_code,
        replicants.saf_street_name
    FROM {{ ref('int__saf_replicant_fields') }} AS replicants
    INNER JOIN lion ON replicants.lionkey = lion.lionkey
)
SELECT
    'error' AS log_level,
    'lion gdb saf street code missing' AS error_category,
    'CSCL-LOG-BYTES-01' AS log_message_id,
    saf_globalid AS globalid,
    saf_source_table AS source_feature_layer,
    'lionkey' AS record_id_type,
    lionkey AS record_id,
    {{ log_message(
        "[SAFSTREETCODE MISSING ERROR] The SAFStreetCode for the SAF {} replicant with a "
        "LIONKEY = {} is empty.  The record will not be included in the output file.",
        ["specaddr", "lionkey"]
    ) }} AS message
FROM replicants
WHERE TRIM(COALESCE(saf_street_code, '')) = ''
UNION ALL
SELECT
    'error' AS log_level,
    'lion gdb saf street name missing' AS error_category,
    'CSCL-LOG-BYTES-02' AS log_message_id,
    saf_globalid AS globalid,
    saf_source_table AS source_feature_layer,
    'lionkey' AS record_id_type,
    lionkey AS record_id,
    {{ log_message(
        "[SAFSTREETNAME MISSING ERROR] The SAFStreetName for the SAF {} replicant with a "
        "SAFStreetCode = {} a LIONKEY = {} is empty.  The record will not be included in "
        "the output file.",
        ["specaddr", "saf_street_code", "lionkey"]
    ) }} AS message
FROM replicants
WHERE
    TRIM(COALESCE(saf_street_code, '')) <> ''
    AND TRIM(COALESCE(saf_street_name, '')) = ''
UNION ALL
SELECT DISTINCT
    'error' AS log_level,
    'lion gdb street name missing' AS error_category,
    'CSCL-LOG-BYTES-03' AS log_message_id,
    globalid,
    source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    segmentid::VARCHAR AS record_id,
    {{ log_message(
        "[NAME TABLE ERROR] The name for the {} feature with a SEGMENTID = {} was not found "
        "in the StreetName table or the Featurename table.  The record will not be included "
        "in the output file.",
        ["source_table", "segmentid"]
    ) }} AS message
FROM lion
WHERE TRIM(COALESCE(street, '')) = ''
