-- CSCL-LOG-OURS-24, every build: how many LION gdb lion layer rows have a non-SAF Join_ID,
-- which the spec (2.7.3) and prod's ETL end in two spaces and prod's lion layer strips
-- (CSCL-LION-10, var pad_lion_join_id), and whether the build keeps the padding. A SAF
-- replicant's Join_ID ends in its SAF type, so has none either way. One row, also when the
-- count is 0.
WITH changed AS (
    SELECT "SegmentID"
    FROM {{ ref('gdb_lion') }}
    WHERE "SpecAddr" IS NULL
)
SELECT
    'warning' AS log_level,
    'join id padding' AS error_category,
    'CSCL-LOG-OURS-24' AS log_message_id,
    NULL::VARCHAR AS globalid,
    NULL::VARCHAR AS source_feature_layer,
    NULL::VARCHAR AS record_id_type,
    NULL::VARCHAR AS record_id,
    {% if var('pad_lion_join_id') -%}
    {{ log_message(
        "{} LION gdb lion layer rows have a non-SAF Join_ID, which the spec ends in two spaces. "
        "Join_ID padding on the lion layer is ON in this build (pad_lion_join_id = true): "
        "they end in two spaces, as the spec says, where prod's lion layer strips them; "
        "the altnames layer keeps the padding either way (CSCL-LION-10)",
        ["count(*)"]
    ) }}
    {%- else -%}
    {{ log_message(
        "{} LION gdb lion layer rows have a non-SAF Join_ID, which the spec ends in two spaces. "
        "Join_ID padding on the lion layer is OFF in this build (pad_lion_join_id = false): "
        "they have none, as prod's lion layer, where the spec pads them; "
        "the altnames layer keeps the padding either way (CSCL-LION-10)",
        ["count(*)"]
    ) }}
    {%- endif %} AS message,
    count(*) AS record_count,
    NULL::VARCHAR AS output_filename
FROM changed
