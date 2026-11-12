-- one row per project location. The Capital Dashboard's project number uses the same codes as ACEP in the budgets
-- data.
SELECT
    project_number AS acep,
    project_number_sequence::INTEGER AS location_number,
    plan_series::INTEGER AS plan_series,
    capital_plan,
    agency_name,
    category_description AS category,
    element_description AS element,
    project_description,
    location_indicator,
    geom
FROM {{ source('sources', 'mta_capital_project_locations') }}
