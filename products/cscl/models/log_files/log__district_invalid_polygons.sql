-- CSCL-LOG-OURS-20, every build: how many nybid, nyhez and nycdwi features have an invalid
-- source polygon, which prod publishes as stored and ours can publish made valid
-- (CSCL-DISTRICTS-04, var make_valid_district_polygons), which ones, and which the build
-- publishes. One row, also when the count is 0.
SELECT
    'warning' AS log_level,
    'invalid district polygon' AS error_category,
    'CSCL-LOG-OURS-20' AS log_message_id,
    NULL::VARCHAR AS globalid,
    NULL::VARCHAR AS source_feature_layer,
    NULL::VARCHAR AS record_id_type,
    NULL::VARCHAR AS record_id,
    {% if var('make_valid_district_polygons') -%}
    {{ log_message(
        "{} district features have an invalid source polygon ({}). "
        "Making them valid is ON in this build (make_valid_district_polygons = true): "
        "they are published valid, where prod publishes them as stored (CSCL-DISTRICTS-04)",
        ["count(*)", "coalesce(string_agg(source_layer || ' ' || key_value, ', ' ORDER BY source_layer, key_value), 'none')"]
    ) }}
    {%- else -%}
    {{ log_message(
        "{} district features have an invalid source polygon ({}). "
        "Making them valid is OFF in this build (make_valid_district_polygons = false): "
        "they are published as stored, as prod does (CSCL-DISTRICTS-04)",
        ["count(*)", "coalesce(string_agg(source_layer || ' ' || key_value, ', ' ORDER BY source_layer, key_value), 'none')"]
    ) }}
    {%- endif %} AS message,
    count(*) AS record_count,
    NULL::VARCHAR AS output_filename
FROM {{ ref('int__district_invalid_source_polygons') }}
