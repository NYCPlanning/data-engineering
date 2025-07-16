WITH pops AS (
    SELECT
        variable_type,
        variable_id,
        bbl,
        raw_geom
    FROM
        {{ ref('stg__pops') }}
),

pluto AS (
    SELECT
        bbl,
        geom
    FROM {{ ref('stg__pluto') }}
),

-- matching pops with pluto on geometry because pops has null BBL values
pops_with_pluto AS (
    SELECT
        po.variable_type,
        po.variable_id,
        po.raw_geom,
        pl.geom AS lot_geom
    FROM pops AS po
    LEFT JOIN pluto AS pl ON ST_Within(po.raw_geom, pl.geom)
)

SELECT
    variable_type,
    variable_id,
    raw_geom,
    lot_geom
FROM pops_with_pluto
ORDER BY variable_type ASC, variable_id ASC
