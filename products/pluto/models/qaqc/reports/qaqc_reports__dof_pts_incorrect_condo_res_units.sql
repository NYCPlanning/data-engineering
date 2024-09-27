WITH active_condo_unitsres_corrections AS (
    SELECT bbl
    FROM {{ ref('qaqc_int__active_condo_bbl_unitsres_corrections') }}
),

filtered_dof_pts_propmaster AS (
    SELECT boro || tb || tl AS bbl
    FROM {{ ref('dof_pts_propmaster') }}
    WHERE primebbl IN (
        SELECT bbl FROM active_condo_unitsres_corrections
    )
)

SELECT * FROM
    {{ source("recipe_sources", "pluto_pts") }}
WHERE (boro || block || lot) IN (
    SELECT bbl FROM filtered_dof_pts_propmaster
)
