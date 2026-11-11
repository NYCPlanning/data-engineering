-- Housing units at the end of 2025, from the 2020 census and the Housing Database completions since, the same
-- way as the district and NTA series. Floored at zero: in a few tracts DOB's recorded units in demolished buildings
-- exceed what the 2020 census counted there, which would leave a negative count.
WITH series AS (
    {{ housing_units_by_date('int_census_tract_spine', 'int_census_tract_census_metrics', 'boroct2020') | indent(4) }}
)

SELECT
    boroct2020,
    greatest(housing_units_from_2020_census, 0) AS housing_units_2025_end
FROM series
WHERE as_of_date = DATE '2025-12-31'
