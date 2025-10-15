{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['segmentid']},
    ]
) }}

WITH segment_offsets AS (
    SELECT * FROM {{ ref("int__segment_offsets") }}
)
SELECT
    so.lionkey_dev,
    so.segmentid,
    leftzip.zip_code AS l_zip,
    rightzip.zip_code AS r_zip
FROM segment_offsets AS so
-- using a cte around reference can confus the postgres compiler to not use index
LEFT JOIN {{ source("recipe_sources", "dcp_cscl_zipcode") }} AS leftzip
    ON st_within(so.left_offset_point, leftzip.geom)
LEFT JOIN {{ source("recipe_sources", "dcp_cscl_zipcode") }} AS rightzip
    ON st_within(so.right_offset_point, rightzip.geom)
