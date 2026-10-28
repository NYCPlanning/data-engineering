WITH acs AS (
    SELECT
        spine.borocd,
        acs.pff_variable,
        acs.estimate,
        acs.margin_of_error
    FROM {{ ref('stg_pff__acs') }} AS acs
    INNER JOIN {{ ref('int_cd_spine') }} AS spine ON acs.cdta2020 = spine.cdta2020
    WHERE acs.pff_variable IN ('pop_1', 'hu1', 'ochu1')
)

SELECT
    borocd,
    max(estimate) FILTER (WHERE pff_variable = 'pop_1') AS population_acs_2020_2024,
    max(margin_of_error) FILTER (WHERE pff_variable = 'pop_1') AS population_acs_2020_2024_moe,
    max(estimate) FILTER (WHERE pff_variable = 'hu1') AS housing_units_acs_2020_2024,
    max(margin_of_error) FILTER (WHERE pff_variable = 'hu1') AS housing_units_acs_2020_2024_moe,
    max(estimate) FILTER (WHERE pff_variable = 'ochu1') AS occupied_housing_units_acs_2020_2024,
    max(margin_of_error) FILTER (WHERE pff_variable = 'ochu1') AS occupied_housing_units_acs_2020_2024_moe
FROM acs
GROUP BY borocd
