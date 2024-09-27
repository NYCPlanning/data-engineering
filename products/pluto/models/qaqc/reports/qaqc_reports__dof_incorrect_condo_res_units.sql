WITH active_condo_unitsres_corrections AS (
    SELECT bbl
    FROM {{ ref('qaqc_int__active_condo_bbl_unitsres_corrections') }}
),

dof_condo_units AS (
    SELECT
        primebbl,
        sum(coop_apts) AS coop_apts,
        sum(units) AS units
    FROM {{ ref('pluto_rpad_geo') }}
    WHERE
        primebbl IN (SELECT bbl FROM active_condo_unitsres_corrections)
        AND tl NOT LIKE '75%'
    GROUP BY primebbl
)

SELECT
    l.*,
    r.units_co,
    r.classa_prop,
    r.count_bins,
    l.coop_apts = r.classa_prop AS dof_matches_devdb_units,
    r.classa_prop - l.coop_apts AS diff
FROM dof_condo_units AS l
INNER JOIN {{ ref('qaqc_int__devdb_bbl_units_summary') }} AS r
    ON l.primebbl = r.bbl
