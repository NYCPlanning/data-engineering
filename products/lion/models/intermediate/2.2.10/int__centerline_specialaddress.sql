{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid', 'boroughcode']},
    ]
) }}
WITH altsegment_saf_records AS (
    SELECT * FROM {{ ref("stg__altsegmentdata_saf") }}
),
addresspoints AS (
    SELECT * FROM {{ source("recipe_sources", "dcp_cscl_addresspoints") }}
),
commonplace AS (
    SELECT * FROM {{ source("recipe_sources", "dcp_cscl_commonplace_gdb") }}
),
unioned AS (
    SELECT
        segmentid,
        saftype,
        borough AS boroughcode,
        1 AS source_priority,
        ogc_fid
    FROM altsegment_saf_records
    UNION ALL
    SELECT
        segmentid,
        special_condition AS saftype,
        boroughcode,
        2 AS source_priority,
        ogc_fid
    FROM addresspoints
    UNION ALL
    SELECT
        segmentid,
        saftype,
        boroughcode,
        3 AS source_priority,
        ogc_fid
    FROM commonplace
),
prioritized AS (
    SELECT
        segmentid,
        boroughcode,
        unioned.saftype,
        row_number() OVER (
            PARTITION BY segmentid, boroughcode
            -- very specific fields have priority, as specified in seed table
            -- beyond that, order by source and row number
            ORDER BY p.priority, source_priority, ogc_fid
        ) AS row_number
    FROM unioned
    INNER JOIN {{ ref('seed_saf_priority') }} AS p ON unioned.saftype = p.saftype
)
SELECT
    segmentid,
    boroughcode,
    saftype AS special_address_flag
FROM prioritized
WHERE row_number = 1
