WITH income AS (
    SELECT geography, geoid, estimate, margin_of_error
    FROM {{ ref('stg_pff__acs') }}
    WHERE pff_variable = 'mdhhinc'
)

SELECT
    spine.borocd,
    cdta.estimate AS cdta_median_household_income_acs,
    cdta.margin_of_error AS cdta_median_household_income_acs_moe,
    city.estimate AS citywide_median_household_income_acs
FROM {{ ref('int_cd_spine') }} AS spine
LEFT JOIN income AS cdta ON cdta.geography = 'cdta' AND spine.cdta2020 = cdta.geoid
LEFT JOIN income AS city ON city.geography = 'city'
