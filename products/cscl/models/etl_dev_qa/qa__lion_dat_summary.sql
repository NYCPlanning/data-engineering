{{ config(
    materialized = 'table',
) }}
WITH lion_dat_fields AS (
    SELECT * FROM {{ ref('lion_dat_by_field') }}
),
production_lion AS (
    SELECT * FROM {{ source('production_outputs', 'citywide_lion_dat') }}
),
summary AS (
    SELECT
        -- sqlfluff doesn't like count(DISTINCT (_1, _2)) even though that's the way to do this
        -- noqa: disable=LT01,LT05,LT06,ST08
        COUNT(DISTINCT (dev.boroughcode, dev.face_code, dev.segmentid)) FILTER (WHERE dev.segmentid IS NOT NULL) AS dev_unique_keys,
        COUNT(DISTINCT (prod.boroughcode, prod.face_code, prod.segmentid)) FILTER (WHERE prod.segmentid IS NOT NULL) AS prod_unique_keys,
        COUNT(DISTINCT (dev.boroughcode, dev.face_code, dev.segmentid)) FILTER (WHERE dev.segmentid IS NOT NULL AND prod.segmentid IS NOT NULL) AS keys_in_both,
        -- noqa: disable=LT01,LT05,LT06,ST08
        COUNT(*) FILTER (WHERE prod.segmentid IS NULL) AS missing_key_in_production,
        COUNT(*) FILTER (WHERE dev.segmentid IS NULL) AS missing_key_in_dev
    {%- for col in adapter.get_columns_in_relation(source('production_outputs', 'citywide_lion_dat')) -%}
        {%- if loop.first -%},{% endif %}
        COUNT(*) FILTER (WHERE dev."{{ col.column }}" <> prod."{{ col.column }}") AS "{{ col.column }}"
        {%- if not loop.last -%},{% endif %}
    {%- endfor %}
    FROM lion_dat_fields AS dev
    FULL JOIN production_lion AS prod
        ON
            dev.boroughcode = prod.boroughcode
            AND dev.face_code = prod.face_code
            --AND dev.segment_seqnum = prod.segment_seqnum -- commented out while nonstreetfeatures do not have these generated
            AND dev.segmentid = prod.segmentid
)
SELECT
    (SELECT COUNT(*) FROM lion_dat_fields) AS total_dev_rows,
    (SELECT COUNT(*) FROM production_lion) AS total_prod_rows,
    *
FROM summary
