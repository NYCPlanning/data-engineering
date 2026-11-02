-- CSCL-LOG-OURS-14, every build: how many centerline proto-segments have a face code only
-- in FeatureName, which prod drops (its table-name check compares mismatched cases, so it
-- looks in StreetName twice) and the fix writes with the FeatureName face code
-- (CSCL-LION-22, var fix_featurename_face_code_protos), and whether the fix is on in this
-- build. One row, also when the count is 0.
WITH changed AS (
    SELECT globalid
    FROM {{ ref('int__protosegments') }}
    WHERE is_featurename_face_code
)
SELECT
    'warning' AS log_level,
    'proto face code legacy bug' AS error_category,
    'CSCL-LOG-OURS-14' AS log_message_id,
    NULL::VARCHAR AS globalid,
    'altsegmentdata' AS source_feature_layer,
    NULL::VARCHAR AS record_id_type,
    NULL::VARCHAR AS record_id,
    {% if var('fix_featurename_face_code_protos') -%}
    {{ log_message(
        "{} centerline proto-segments have a face code only in FeatureName. The FeatureName face code "
        "fix is ON in this build (fix_featurename_face_code_protos = true): they are written with it "
        "(CSCL-LOG-LION-15), where prod drops them (CSCL-LION-22)",
        ["COUNT(DISTINCT globalid)"]
    ) }}
    {%- else -%}
    {{ log_message(
        "{} centerline proto-segments have a face code only in FeatureName. The FeatureName face code "
        "fix is OFF in this build (fix_featurename_face_code_protos = false): they are dropped, as "
        "prod (CSCL-LOG-LION-19), not written with it (CSCL-LION-22)",
        ["COUNT(DISTINCT globalid)"]
    ) }}
    {%- endif %} AS message,
    COUNT(DISTINCT globalid) AS record_count,
    NULL::VARCHAR AS output_filename
FROM changed
