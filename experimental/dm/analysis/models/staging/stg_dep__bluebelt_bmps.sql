SELECT
    bmp_id,
    bmp_type,
    watershed,
    status,
    -- 0 where the layer has no year
    nullif(datebuilt, 0) AS year_built,
    borough,
    geom
FROM {{ source('sources', 'dep_bluebelt_bmps') }}
