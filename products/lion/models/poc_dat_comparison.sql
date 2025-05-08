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
{% for col in adapter.get_columns_in_relation(ref('lion_dat_fields')) -%}
    count(*) FILTER (WHERE dev."{{ col.column }}" <> prod."{{ col.column }}") AS "{{ col.column }}"
    {%- if not loop.last -%},{% endif %}
{% endfor -%}
FROM lion_dat_fields AS dev
INNER JOIN production_lion AS prod
    ON
        dev."Borough" = prod."Borough"
        AND dev."Sequence Number" = prod."Sequence Number"
        AND dev."Segment ID" = prod."Segment ID"
