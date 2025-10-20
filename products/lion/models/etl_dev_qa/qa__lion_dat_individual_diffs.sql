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
            WHEN dev."Borough" IS NULL THEN '{"missing in dev": ""}'::jsonb
            WHEN prod."Borough" IS NULL THEN '{"missing in prod": ""}'::jsonb
            ELSE
                /* this somewhat unreadable code produces an object
                   with one entry per field like so:
                    {
                        "Borough": {
                            "match": true,
                            "dev": "1",
                            "prod", "1"
                        },
                        "Left Election District": {
                            "match": false,
                            "dev": "1",
                            "prod": "2"
                        },
                        "Left Assembly District": {
                            "match": false,
                            "dev": "31",
                            "prod": "30"
                        }
                    }
                */
                (
                    '{'
                    {%- for col in adapter.get_columns_in_relation(source('production_outputs', 'citywide_lion')) -%}
                        || '"{{ col.column }}": {"match": ' || (dev."{{ col.column }}" = prod."{{ col.column }}")
                        || ', "dev": "' || dev."{{ col.column }}" || '", "prod": "' || prod."{{ col.column }}" || '"}'
                        {%- if not loop.last -%}|| ',' {% endif %}
                    {%- endfor %}
                    || '}'
                )::jsonb
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
)
SELECT
    _source_table,
    s.borough,
    s.face_code,
    s.sequence_number,
    s.segment_id,
    j.key AS field,
    j.value -> 'dev' AS dev,
    j.value -> 'prod' AS prod
FROM matches_as_jsonb AS s,
    LATERAL JSONB_EACH(columns_with_diffs) AS j
WHERE j.value -> 'match' IS DISTINCT FROM 'true'
ORDER BY borough, face_code, sequence_number
