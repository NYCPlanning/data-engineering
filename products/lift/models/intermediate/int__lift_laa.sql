-- HPD's Limited Affordability Areas has no BBL field either, so like CPDB this is a
-- spatial join: any bbl whose PLUTO lot intersects an LAA polygon gets flagged. The
-- source shapefile carries no attribute data (see stg__hpd_limited_affordability_areas),
-- so there's no area name/id to carry through - just which bbls intersect at all. One
-- row per intersecting bbl; product/lift_supplemented.sql turns presence/absence here
-- into the 'Y'/NULL laa flag the data dictionary specifies.
WITH lift AS (
    SELECT bbl FROM {{ ref('stg__lift_csv') }}
),

pluto AS (
    SELECT bbl, geom FROM {{ ref('stg__pluto') }}
),

laa AS (
    SELECT geom FROM {{ ref('stg__hpd_limited_affordability_areas') }}
),

final AS (
    SELECT DISTINCT lift.bbl
    FROM lift
    INNER JOIN pluto ON lift.bbl = pluto.bbl
    INNER JOIN laa ON ST_INTERSECTS(pluto.geom, laa.geom)
)

SELECT * FROM final
