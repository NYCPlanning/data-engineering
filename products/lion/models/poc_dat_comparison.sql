{{ config(
    materialized = 'table',
) }}
WITH lion_dat_fields AS (
    SELECT * FROM {{ ref('lion_dat_fields') }}
),
production_lion AS (
    SELECT * FROM {{ source('production_outputs', 'citywide_lion') }}
)
SELECT
    count(*) FILTER (WHERE dev."Segment ID" IS NOT NULL) AS total_dev_rows,
    count(*) FILTER (WHERE prod."Segment ID" IS NOT NULL) AS total_prod_rows,
    count(*) FILTER (WHERE dev."Segment ID" IS NOT NULL AND prod."Segment ID" IS NOT NULL) AS keys_in_both,
    count(*) FILTER (WHERE prod."Segment ID" IS NULL) AS missing_key_in_production,
    count(*) FILTER (WHERE dev."Segment ID" IS NULL) AS missing_key_in_dev
{%- for col in adapter.get_columns_in_relation(ref('lion_dat_fields')) -%}
    {%- if loop.first -%},{% endif %}
    count(*) FILTER (WHERE dev."{{ col.column }}" <> prod."{{ col.column }}") AS "{{ col.column }}"
    {%- if not loop.last -%},{% endif %}
{%- endfor %}
FROM lion_dat_fields AS dev
INNER JOIN production_lion AS prod
    ON
        dev."Borough" = prod."Borough"
        AND dev."Face Code" = prod."Face Code"
        AND dev."Sequence Number" = prod."Sequence Number"
        AND dev."Segment ID" = prod."Segment ID"
