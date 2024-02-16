WITH dcp_lion AS (
    SELECT * FROM {{ source('ceqr_survey_sources', 'dcp_lion') }}
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
    'elevated_railway' AS variable,
    street AS id,
    ST_BUFFER(geom, 1500) AS geom
FROM filtered
