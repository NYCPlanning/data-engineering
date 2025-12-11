{{ config(
    materialized = 'table',
) }}
WITH saf AS (
    SELECT * FROM {{ ref('saf_abcegnpx_generic') }}
),
prod AS (
    SELECT * FROM {{ source('production_outputs', 'saf_abcegnpx_generic') }}
)
SELECT -- noqa: LT09
    dev._source_table,
    ARRAY_REMOVE(ARRAY[
        {%- for col in adapter.get_columns_in_relation(source('production_outputs', 'saf_abcegnpx_generic')) -%}
            CASE WHEN dev."{{ col.column }}" <> prod."{{ col.column }}" THEN '{{ col.column }}' END
            {%- if not loop.last -%},{% endif %}
        {%- endfor %}
    ], NULL) AS columns_with_diffs
{%- for col in adapter.get_columns_in_relation(source('production_outputs', 'saf_abcegnpx_generic')) -%}
    {%- if loop.first -%},{% endif %}
    dev."{{ col.column }}" AS "{{ col.column }}_dev",
    prod."{{ col.column }}" AS "{{ col.column }}_prod"
    {%- if not loop.last -%},{% endif %}
{%- endfor %}
FROM saf AS dev
FULL JOIN prod --noqa: ST11
    ON
        dev.boroughcode = prod.boroughcode
        AND dev.face_code = prod.face_code
        --AND dev.segment_seqnum = prod.segment_seqnum -- commented out while nonstreetfeatures do not have these generated
        AND dev.segmentid = prod.segmentid
WHERE
    FALSE
{%- for col in adapter.get_columns_in_relation(source('production_outputs', 'saf_abcegnpx_generic')) -%}
    {%- if loop.first %} OR{% endif %}
    (dev."{{ col.column }}" IS DISTINCT FROM prod."{{ col.column }}") OR
    {%- if loop.last %} FALSE{% endif %}
{%- endfor %}
ORDER BY dev.boroughcode, dev.face_code, dev.segment_seqnum
