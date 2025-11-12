{{ config(
    materialized = 'table',
) }}
WITH lion_dat_fields AS (
    SELECT * FROM {{ ref('lion_dat_by_field') }}
),
production_lion AS (
    SELECT * FROM {{ source('production_outputs', 'citywide_lion') }}
),
matches_as_jsonb AS (
    SELECT
        dev._source_table,
        COALESCE(dev."Borough", prod."Borough") AS borough,
        COALESCE(dev."Face Code", prod."Face Code") AS face_code,
        COALESCE(dev."Sequence Number", prod."Sequence Number") AS sequence_number,
        COALESCE(dev."Segment ID", prod."Segment ID") AS segment_id,
        CASE
            WHEN dev."Borough" IS NULL THEN '{"missing in dev": {}}'::jsonb
            WHEN prod."Borough" IS NULL THEN '{"missing in prod": {}}'::jsonb
            ELSE
                JSONB_OBJECT(ARRAY[
                    {%- for col in adapter.get_columns_in_relation(source('production_outputs', 'citywide_lion')) -%}
                        '{{ col.column }}', JSON_BUILD_OBJECT(
                            'match', dev."{{ col.column }}" = prod."{{ col.column }}",
                            'dev', dev."{{ col.column }}",
                            'prod', prod."{{ col.column }}"
                        )::text
                        {%- if not loop.last -%},{% endif %}
                    {%- endfor %}
                ])
        END AS columns_with_diffs
    FROM lion_dat_fields AS dev
    FULL JOIN production_lion AS prod
        ON
            dev."Borough" = prod."Borough"
            AND dev."Face Code" = prod."Face Code"
            --AND dev."Sequence Number" = prod."Sequence Number" -- commented out while nonstreetfeatures do not have these generated
            AND dev."Segment ID" = prod."Segment ID"
    WHERE
        FALSE
    {%- for col in adapter.get_columns_in_relation(source('production_outputs', 'citywide_lion')) -%}
        {%- if loop.first %} OR{% endif %}
        (dev."{{ col.column }}" IS DISTINCT FROM prod."{{ col.column }}") OR
        {%- if loop.last %} FALSE{% endif %}
    {%- endfor %}
),
recast AS (
    SELECT
        s._source_table,
        s.borough,
        s.face_code,
        s.sequence_number,
        s.segment_id,
        j.key AS field,
        (j.value::jsonb #>> '{}')::jsonb AS jsonb_values
    FROM matches_as_jsonb AS s,
        LATERAL JSONB_EACH(columns_with_diffs) AS j
)
SELECT
    _source_table,
    borough,
    face_code,
    sequence_number,
    segment_id,
    field,
    jsonb_values -> 'dev' AS dev,
    jsonb_values -> 'prod' AS prod
FROM recast
WHERE jsonb_values -> 'match' IS DISTINCT FROM 'true'
ORDER BY borough, face_code, sequence_number
