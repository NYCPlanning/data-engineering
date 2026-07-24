{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

WITH clipped AS (
    SELECT
        d.cb AS "CB2020",
        d.borocode AS "BoroCode",
        b.boroname AS "BoroName",
        d.ct AS "CT2020",
        d.bctcb AS "BCTCB2020",
        '36' || b.fips || d.ct || d.cb AS "GEOID",
        {{ clipped_geom('d.geom') }} AS geom
    FROM {{ ref('stg__censusblock2020') }} AS d
    INNER JOIN {{ ref('stg__borough') }} AS b ON d.borocode = b.borocode
    {{ clip_to_shoreline('d.geom') }}
)

SELECT
    *,
    st_perimeter(geom) AS "SHAPE_Length",
    st_area(geom) AS "SHAPE_Area"
FROM clipped
WHERE NOT st_isempty(geom)
