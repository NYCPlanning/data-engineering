-- The bbl <-> rezoning_area geo-match itself, factored out of int__lift_rezoning_tracker
-- so it can be reused (e.g. ad hoc QA queries joining back to bbl/rezoning geometry)
-- without recomputing the join. One row per (bbl, rezoning_area) - a bbl can match more
-- than one rezoning area if their mapped geometries overlap. See
-- int__lift_rezoning_tracker for how this feeds the commitment counts.
WITH crosswalk AS (
    SELECT DISTINCT rezoning_area, ulurp_no
    FROM {{ ref('rezoning_areas') }}
    WHERE ulurp_no IS NOT NULL
),

rezoning_geom AS (
    SELECT
        crosswalk.rezoning_area,
        crosswalk.ulurp_no,
        zoningmapamendments.geom
    FROM crosswalk
    INNER JOIN {{ ref('stg__dcp_zoningmapamendments') }} AS zoningmapamendments
        ON zoningmapamendments.ulurpno = crosswalk.ulurp_no
),

lift AS (
    SELECT bbl FROM {{ ref('stg__lift_csv') }}
),

pluto AS (
    SELECT bbl, geom FROM {{ ref('stg__pluto') }}
),

final AS (
    SELECT DISTINCT
        lift.bbl,
        rezoning_geom.rezoning_area,
        rezoning_geom.ulurp_no
    FROM lift
    INNER JOIN pluto ON lift.bbl = pluto.bbl
    INNER JOIN rezoning_geom ON ST_INTERSECTS(pluto.geom, rezoning_geom.geom)
)

SELECT * FROM final
