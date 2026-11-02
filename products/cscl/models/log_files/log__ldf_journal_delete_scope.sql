-- CSCL-LOG-OURS-11, every build: how many LDF G and P records have an ID deleted in the
-- journal but not on their node pair, which prod drops and the fix keeps (CSCL-LDF-01, var
-- fix_ldf_journal_delete_scope), and whether the fix is on in this build. Each one also
-- moves the record number of every record after it (CSCL-LDF-03). One row, also when the
-- count is 0.
WITH changed AS (
    SELECT record_type
    FROM {{ ref('int__ldf_segments') }}
    WHERE id_deleted_in_journal IS DISTINCT FROM id_deleted_on_node_pair
)
SELECT
    'warning' AS log_level,
    'LDF legacy bug' AS error_category,
    'CSCL-LOG-OURS-11' AS log_message_id,
    NULL::VARCHAR AS globalid,
    NULL::VARCHAR AS source_feature_layer,
    NULL::VARCHAR AS record_id_type,
    NULL::VARCHAR AS record_id,
    {% if var('fix_ldf_journal_delete_scope') -%}
    {{ log_message(
        "{} LDF records ({} G, {} P) have a generic or physical ID that is deleted in the journal, "
        "but not on their node pair. "
        "The LDF journal delete scope fix is ON in this build (fix_ldf_journal_delete_scope = true): "
        "they are written, where prod drops them (CSCL-LDF-01)",
        ["COUNT(*)", "COUNT(*) FILTER (WHERE record_type = 'G')", "COUNT(*) FILTER (WHERE record_type = 'P')"]
    ) }}
    {%- else -%}
    {{ log_message(
        "{} LDF records ({} G, {} P) have a generic or physical ID that is deleted in the journal, "
        "but not on their node pair. "
        "The LDF journal delete scope fix is OFF in this build (fix_ldf_journal_delete_scope = false): "
        "they are dropped, as prod (CSCL-LDF-01)",
        ["COUNT(*)", "COUNT(*) FILTER (WHERE record_type = 'G')", "COUNT(*) FILTER (WHERE record_type = 'P')"]
    ) }}
    {%- endif %} AS message,
    COUNT(*) AS record_count,
    NULL::VARCHAR AS output_filename
FROM changed
