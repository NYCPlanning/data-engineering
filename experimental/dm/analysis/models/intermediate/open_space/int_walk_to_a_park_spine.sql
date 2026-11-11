SELECT
    row_number() OVER () AS served_lot_number,
    geom
FROM {{ ref('stg_dpr__walk_to_a_park') }}
