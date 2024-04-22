{{ config(
    materialized = 'table'
) }}
WITH flags_zoning AS (
    SELECT * FROM {{ ref('int_flags__zoning') }}
),

pluto AS (
    SELECT * FROM {{ ref('stg__pluto') }}
),

expected_flags_zoning AS (
    SELECT * FROM {{ ref('test_expected__flags_zoning') }}
),

flags_zoning_districts AS (
    SELECT * FROM flags_zoning WHERE variable_type = 'zoning_districts'
),

flags_with_zonedists AS (
    SELECT
        pluto.bbl,
        pluto.zonedist1,
        pluto.zonedist2,
        pluto.zonedist3,
        pluto.zonedist4,
        flags_zoning_districts.variable_id AS "Zoning Districts"
    FROM pluto LEFT JOIN flags_zoning_districts
        ON pluto.bbl = flags_zoning_districts.bbl
),

final AS (
    SELECT
        expected_flags_zoning.lot_label,
        flags_with_zonedists.*
    FROM flags_with_zonedists INNER JOIN expected_flags_zoning
        ON flags_with_zonedists.bbl = expected_flags_zoning.bbl
)

SELECT * FROM final ORDER BY lot_label ASC, bbl ASC
