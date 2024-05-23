WITH dcp_lion AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_lion') }}
),

filtered AS (
    SELECT
        street,
        ST_UNION(shape) AS geom
    FROM dcp_lion
    WHERE row_type IN ('2', '3', '4', '5', '6', '7')
    GROUP BY street
)

SELECT
    'exposed_railway' AS flag_variable_type,
    'exposed_railways' AS variable_type,
    street AS variable_id,
    geom AS raw_geom,
    ST_BUFFER(geom, 1500) AS buffer_geom
FROM filtered
