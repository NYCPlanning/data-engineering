{{ config(
    materialized = 'table',
) }}
WITH lion_dat_fields AS (
    SELECT * FROM {{ ref('lion_dat_by_field') }}
),
production_lion AS (
    SELECT * FROM {{ source('production_outputs', 'citywide_lion') }}
)
SELECT -- noqa: LT09
    dev._source_table,
    ARRAY_REMOVE(ARRAY[
        {%- for col in adapter.get_columns_in_relation(source('production_outputs', 'citywide_lion')) -%}
            CASE WHEN dev."{{ col.column }}" <> prod."{{ col.column }}" THEN '{{ col.column }}' END
            {%- if not loop.last -%},{% endif %}
        {%- endfor %}
    ], NULL) AS columns_with_diffs
{%- for col in adapter.get_columns_in_relation(source('production_outputs', 'citywide_lion')) -%}
    {%- if loop.first -%},{% endif %}
    dev."{{ col.column }}" AS "{{ col.column }}_dev",
    prod."{{ col.column }}" AS "{{ col.column }}_prod"
    {%- if not loop.last -%},{% endif %}
{%- endfor %}
FROM lion_dat_fields AS dev
FULL JOIN production_lion AS prod --noqa: ST11
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
ORDER BY dev."Borough", dev."Face Code", dev."Sequence Number"
