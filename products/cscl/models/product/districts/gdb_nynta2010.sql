{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- Borough is assigned by point-on-surface containment per the ETL spec's centroid
-- rule; point-on-surface keeps the probe inside concave shapes.

WITH clipped AS (
    SELECT
        b.borocode::int AS "BoroCode",
        b.boroname AS "BoroName",
        b.fips AS "CountyFIPS",
        d.neighborhood_code AS "NTACode",
        npc.neighborhood_name AS "NTAName",
        {{ clipped_geom('d.geom') }} AS geom
    FROM {{ ref('stg__neighborhood') }} AS d
    INNER JOIN {{ ref('stg__borough') }} AS b
        ON st_contains(b.geom, st_pointonsurface(d.geom))
    LEFT JOIN (
        SELECT DISTINCT
            neighborhood_code,
            neighborhood_name
        FROM {{ ref('stg__neighborhoodpumacodes') }}
    ) AS npc ON d.neighborhood_code = npc.neighborhood_code
    {{ clip_to_shoreline('d.geom') }}
)

SELECT
    *,
    st_perimeter(geom) AS "SHAPE_Length",
    st_area(geom) AS "SHAPE_Area"
FROM clipped
WHERE NOT st_isempty(geom)
