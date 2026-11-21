-- KPDB's census baseline and PFF's 2020 count should be the same number, or its NTA projection doesn't sit on the same
-- baseline as the housing series it's added to.
SELECT
    kpdb.nta2020,
    kpdb.units_2020_census AS kpdb_units_2020_census,
    pff.housing_units_2020_census AS pff_housing_units_2020_census
FROM {{ ref('stg_kpdb__cpp_housing_growth_nta') }} AS kpdb
FULL JOIN {{ ref('int_nta_census_metrics') }} AS pff ON kpdb.nta2020 = pff.nta2020
WHERE kpdb.units_2020_census IS DISTINCT FROM pff.housing_units_2020_census
