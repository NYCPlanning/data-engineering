-- CSCL-LOG-OURS-21, every build: how many 2010 census tracts have a source BOROCT or
-- NEIGHBORHOOD_CODE other than their borough and tract's, which prod's nyct2010 and
-- nyct2010wi copy and ours can key by borough and tract instead (CSCL-DISTRICTS-10, var
-- key_tracts_by_borough), which tracts, and which keys the build writes. One row, also
-- when the count is 0.
WITH changed AS (
    SELECT
        boroct,
        source_boroct,
        nta_code_prod
    FROM {{ ref('int__ct2010_keys') }}
    WHERE
        source_boroct IS DISTINCT FROM boroct
        OR nta_code_prod IS DISTINCT FROM nta_code_borough
)

SELECT
    'warning' AS log_level,
    'tract source key' AS error_category,
    'CSCL-LOG-OURS-21' AS log_message_id,
    NULL::VARCHAR AS globalid,
    'nyct2010' AS source_feature_layer,
    NULL::VARCHAR AS record_id_type,
    NULL::VARCHAR AS record_id,
    {% if var('key_tracts_by_borough') -%}
    {{ log_message(
        "{} 2010 census tracts have a source BOROCT or NEIGHBORHOOD_CODE that isn't their borough and tract's ({}). "
        "Keying tracts by borough is ON in this build (key_tracts_by_borough = true): "
        "nyct2010 and nyct2010wi key them by borough and tract, where prod copies the source's (CSCL-DISTRICTS-10)",
        ["count(*)", "coalesce(string_agg(boroct || ' as ' || coalesce(source_boroct, 'blank') || ' ' || coalesce(nta_code_prod, 'blank'), ', ' ORDER BY boroct), 'none')"]
    ) }}
    {%- else -%}
    {{ log_message(
        "{} 2010 census tracts have a source BOROCT or NEIGHBORHOOD_CODE that isn't their borough and tract's ({}). "
        "Keying tracts by borough is OFF in this build (key_tracts_by_borough = false): "
        "nyct2010 and nyct2010wi copy the source's, as prod does (CSCL-DISTRICTS-10)",
        ["count(*)", "coalesce(string_agg(boroct || ' as ' || coalesce(source_boroct, 'blank') || ' ' || coalesce(nta_code_prod, 'blank'), ', ' ORDER BY boroct), 'none')"]
    ) }}
    {%- endif %} AS message,
    count(*) AS record_count,
    NULL::VARCHAR AS output_filename
FROM changed
