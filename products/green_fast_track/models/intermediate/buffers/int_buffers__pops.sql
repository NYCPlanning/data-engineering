-- int_buffers__pops.sql

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
        COALESCE(pl.geom, po.raw_geom) AS raw_geom
    FROM pops AS po
    LEFT JOIN pluto AS pl ON ST_WITHIN(po.raw_geom, pl.geom)

),

-- create buffer around tax lot (bbl_geom column). 
-- If tax lot is null, create buffer around point (geom column)
final AS (
    SELECT
        variable_type,
        variable_id,
        raw_geom,
        ST_BUFFER(raw_geom, 75) AS buffer
    FROM pops_with_pluto
)

SELECT * FROM final
