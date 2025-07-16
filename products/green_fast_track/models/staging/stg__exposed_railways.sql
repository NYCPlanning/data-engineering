WITH dcp_lion AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_lion') }}
),

filtered AS (
    SELECT
        street,
        st_union(shape) AS geom
    FROM dcp_lion
    WHERE row_type IN ('2', '3', '4', '5', '6', '7')
    GROUP BY street
)

SELECT
    'exposed_railways' AS variable_type,
    street AS variable_id,
    st_multi(geom) AS raw_geom
FROM filtered
