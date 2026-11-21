-- Parsing follows products/lift/models/staging/stg__dcp_housing_ahft.sql
SELECT
    {{ borocd_from_cd_code('"Community District"') }} AS borocd,
    replace("Housing Units (2020 Census)", ',', '')::bigint AS ahft_housing_units_2020_census,
    replace(
        "Total Number Of Housing Units At The Start Of This Cycle (Denominator)", ',', ''
    )::bigint AS ahft_housing_units_start_of_cycle,
    replace(
        "Total Number Of New Affordable Dwelling Units In This Cycle (Numerator)", ',', ''
    )::bigint AS ahft_new_affordable_units,
    "AHFT Rank"::integer AS ahft_rank
FROM {{ source('sources', 'dcp_housing_ahft') }}
