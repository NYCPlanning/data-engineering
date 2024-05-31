WITH pluto AS (
    SELECT
        coalesce(zonedist1, 'NULL') AS zonedist1,
        coalesce(zonedist2, 'NULL') AS zonedist2,
        coalesce(zonedist3, 'NULL') AS zonedist3,
        coalesce(zonedist4, 'NULL') AS zonedist4
    FROM {{ ref('stg__pluto') }}
),

group_by_rollup AS (
    SELECT
        zonedist1,
        zonedist2,
        zonedist3,
        zonedist4,
        count(*) AS zoning_combo_count
    FROM pluto
    GROUP BY ROLLUP(zonedist1, zonedist2, zonedist3, zonedist4) -- noqa: PRS, CP03
)

SELECT
    coalesce(zonedist1, 'ALL') AS zonedist1,
    coalesce(zonedist2, 'ALL') AS zonedist2,
    coalesce(zonedist3, 'ALL') AS zonedist3,
    coalesce(zonedist4, 'ALL') AS zonedist4,
    zoning_combo_count
FROM group_by_rollup
ORDER BY zonedist1, zonedist2, zonedist3, zonedist4
