-- Capital Projects Database doesn't have a BBL field, so we join spatially:
-- every CPDB project whose geometry intersects a lot's PLUTO geometry counts
-- toward that lot's project count/spend. Points and poly are unioned here -
-- confirmed (via cpdb's own pipeline: cpdb_projects_pts/poly are cpdb_projects_shp
-- filtered by ST_GeometryType) that they're a strict partition of one canonical
-- project table with zero overlapping project_ids, so UNION ALL can't double-count
-- a project appearing in both.
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
        description,
        spent_total,
        geom
    FROM {{ ref('stg__cpdb_points') }}

    UNION ALL

    SELECT
        project_id,
        description,
        spent_total,
        geom
    FROM {{ ref('stg__cpdb_poly') }}
),

intersections AS (
    SELECT
        lift.bbl,
        cpdb.project_id,
        cpdb.description,
        cpdb.spent_total
    FROM lift
    INNER JOIN pluto ON lift.bbl = pluto.bbl
    INNER JOIN cpdb ON ST_INTERSECTS(pluto.geom, cpdb.geom)
),

final AS (
    SELECT
        bbl,
        COUNT(DISTINCT project_id) AS cp_projects,
        SUM(spent_total) AS cp_spent_total,
        -- ids and descriptions are both ordered by project_id, so position i in one
        -- list corresponds to position i in the other.
        TO_JSON(LIST(DISTINCT project_id ORDER BY project_id)) AS cp_project_ids,
        TO_JSON(LIST(description ORDER BY project_id)) AS cp_project_descriptions
    FROM intersections
    GROUP BY bbl
)

SELECT * FROM final
