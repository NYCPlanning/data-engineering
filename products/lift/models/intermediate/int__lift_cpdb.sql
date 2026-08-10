-- Capital Projects Database doesn't have a BBL field, so we join spatially:
-- every CPDB point that falls within a lot's PLUTO geometry counts toward that
-- lot's project count/spend. Starting with the CPDB *points* layer per request;
-- the polygon layer (cpdb_projects_poly) isn't joined here yet.
WITH lift AS (
    SELECT bbl FROM {{ ref('stg__lift_csv') }}
),

pluto AS (
    SELECT
        bbl,
        geom
    FROM {{ ref('stg__pluto') }}
),

cpdb AS (
    SELECT
        project_id,
        spent_total,
        geom
    FROM {{ ref('stg__cpdb_points') }}
),

intersections AS (
    SELECT
        lift.bbl,
        cpdb.project_id,
        cpdb.spent_total
    FROM lift
    INNER JOIN pluto ON lift.bbl = pluto.bbl
    INNER JOIN cpdb ON ST_INTERSECTS(pluto.geom, cpdb.geom)
),

final AS (
    SELECT
        bbl,
        COUNT(DISTINCT project_id) AS cp_projects,
        SUM(spent_total) AS cp_spent_total
    FROM intersections
    GROUP BY bbl
)

SELECT * FROM final
