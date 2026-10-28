-- Recomputing KPDB's 2015-2024 completions from the same HDB version checks that
-- int_cd_hdb_completion_metrics sums HDB the way KPDB does.
SELECT
    kpdb.borocd,
    kpdb.completed_units_2015_2024 AS kpdb_completed_units_2015_2024,
    coalesce(hdb.hdb_net_completed_units_2015_2024, 0) AS hdb_net_completed_units_2015_2024
FROM {{ ref('stg_kpdb__cpp_housing_growth_cd') }} AS kpdb
LEFT JOIN {{ ref('int_cd_hdb_completion_metrics') }} AS hdb ON kpdb.borocd = hdb.borocd
WHERE kpdb.completed_units_2015_2024 != coalesce(hdb.hdb_net_completed_units_2015_2024, 0)
