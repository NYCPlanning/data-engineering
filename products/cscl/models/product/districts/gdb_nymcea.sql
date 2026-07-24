{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- Minor Census Economic Areas are not modelled in CSCL; they are dissolved from the
-- MCEA attribute on 2000 census tracts. Clipping splits some areas into disjoint
-- pieces and prod writes each as its own feature, so the dissolved groups are
-- exploded back to singlepart (115 groups -> 122 features). No water-included
-- version is required.

WITH dissolved AS (
    SELECT
        borocode,
        mcea,
        st_union(geom) AS geom
    FROM {{ ref('stg__censustract2000') }}
    WHERE mcea IS NOT NULL
    GROUP BY borocode, mcea
),

clipped AS (
    SELECT
        d.borocode,
        d.mcea,
        {{ clipped_geom('d.geom') }} AS geom
    FROM dissolved AS d
    {{ clip_to_shoreline('d.geom') }}
),

singlepart AS (
    SELECT
        c.borocode AS "BOROCODE",
        c.mcea AS "MCEA",
        b.boroname AS "BoroName",
        st_multi((st_dump(c.geom)).geom) AS geom
    FROM clipped AS c
    INNER JOIN {{ ref('stg__borough') }} AS b ON c.borocode = b.borocode
    WHERE NOT st_isempty(c.geom)
)

-- prod spells these mixed-case on this layer alone; every other feature class
-- uses SHAPE_Length/SHAPE_Area
SELECT
    *,
    st_perimeter(geom) AS "Shape_Length",
    st_area(geom) AS "Shape_Area"
FROM singlepart
