-- KPDB's census baseline and PFF's 2020 count should be the same number. A row here means
-- the two sources disagree, so KPDB's projection doesn't sit on the same baseline as the housing
-- series it's added to.
SELECT
    kpdb.borocd,
    kpdb.units_2020_census AS kpdb_units_2020_census,
    pff.housing_units_2020_census AS pff_housing_units_2020_census
FROM {{ ref('stg_kpdb__cpp_housing_growth_cd') }} AS kpdb
FULL JOIN {{ ref('int_cd_census_metrics') }} AS pff ON kpdb.borocd = pff.borocd
WHERE kpdb.units_2020_census IS DISTINCT FROM pff.housing_units_2020_census
