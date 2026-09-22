-- lift_supplemented plus each bbl's PLUTO lot geometry, for the gdb export. Kept separate
-- so lift_supplemented.csv doesn't carry a geometry column.
WITH lift AS (
    SELECT * FROM {{ ref('lift_supplemented') }}
),

pluto AS (
    SELECT bbl, geom FROM {{ ref('stg__pluto') }}
)

SELECT
    lift.*,
    pluto.geom
FROM lift
LEFT JOIN pluto ON lift.bbl = pluto.bbl
