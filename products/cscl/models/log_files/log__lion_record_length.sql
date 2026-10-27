-- LION .dat records with a field value longer than its slot (CSCL-LOG-LION-21,
-- ExtractorClass.cs:2026-2029). Prod writes the record anyway, longer than the 400
-- characters Geosupport reads, and logs its length. We always write 400 characters:
-- format_lion_text cuts the value to its slot (lion_dat_by_field; CSCL-LION-23), so this logs the length
-- prod would have written and the fields we cut.
{%- set fields_query -%}
    SELECT field_name, field_length
    FROM {{ ref('text_formatting__lion_dat') }}
    WHERE field_name NOT LIKE 'filler_%'
{%- endset -%}
{%- set fields = run_query(fields_query) if execute else [] %}

WITH lion AS (
    SELECT * FROM {{ ref('int__lion') }}
    WHERE include_in_geosupport_lion
),
overflows AS (
    SELECT
        globalid,
        source_table,
        boroughcode || face_code || segment_seqnum AS lionkey,
        0
        {%- for f in fields %}
            + GREATEST(LENGTH(COALESCE({{ f['field_name'] }}::TEXT, '')) - {{ f['field_length'] }}, 0)
        {%- endfor %}
            AS overflow,
        CONCAT_WS(
            ', ',
            {%- for f in fields %}
                CASE WHEN LENGTH({{ f['field_name'] }}::TEXT) > {{ f['field_length'] }} THEN '{{ f['field_name'] }}' END
                {{- "," if not loop.last }}
            {%- endfor %}
        ) AS fields_cut
    FROM lion
)
SELECT
    'error' AS log_level,
    'record length' AS error_category,
    'CSCL-LOG-LION-21' AS log_message_id,
    globalid,
    source_table AS source_feature_layer,
    'lionkey' AS record_id_type,
    lionkey AS record_id,
    {{ log_message(
        "[INCORRECT RECORD LENGTH] The output record for the {} feature with a GlobalID = {} "
        "is only {} characters long.  The required output length is 400. (cut to fit: {})",
        ["source_table", "globalid", "400 + overflow", "fields_cut"]
    ) }} AS message
FROM overflows
WHERE overflow > 0
