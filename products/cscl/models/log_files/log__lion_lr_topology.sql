-- CSCL-LOG-OURS-08, every build: how many LION records (and segments) get a different side
-- AtomicPolygon from the AP topology than from prod's 2 ft probe (CSCL-LION-16), and
-- whether the build takes the topology's (fix_lion_lr_from_topology). One row, also when
-- the count is 0; the records are in log_lion_lr_topology.csv.
SELECT
    'warning' AS log_level,
    'lion side atomic polygon method' AS error_category,
    'CSCL-LOG-OURS-08' AS log_message_id,
    NULL AS globalid,
    NULL AS source_feature_layer,
    NULL AS record_id_type,
    NULL AS record_id,
    {% if var('fix_lion_lr_from_topology') -%}
    {{ log_message(
        "{} LION records ({} segments) get a different left or right AtomicPolygon from the "
        "AtomicPolygon topology than from prod's 2 ft probe. Side APs from the topology are ON "
        "in this build (fix_lion_lr_from_topology = true): they take the topology's, and the "
        "fields derived from it, where prod takes the probe's (CSCL-LION-16); see "
        "log_lion_lr_topology.csv",
        ["COUNT(*)", "COUNT(DISTINCT record_id)"]
    ) }}
    {%- else -%}
    {{ log_message(
        "{} LION records ({} segments) get a different left or right AtomicPolygon from the "
        "AtomicPolygon topology than from prod's 2 ft probe. Side APs from the topology are OFF "
        "in this build (fix_lion_lr_from_topology = false): they take the probe's, as prod, "
        "not the topology's (CSCL-LION-16); see log_lion_lr_topology.csv",
        ["COUNT(*)", "COUNT(DISTINCT record_id)"]
    ) }}
    {%- endif %} AS message,
    COUNT(*) AS record_count,
    NULL AS output_filename
FROM {{ ref('log_lion_lr_topology') }}
