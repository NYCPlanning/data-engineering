SELECT
    borocd,
    ahft_rank,
    -- the cutoff LIFT uses for its ahft flag
    ahft_rank <= 12 AS is_ahft_district,
    ahft_housing_units_start_of_cycle,
    ahft_new_affordable_units
FROM {{ ref('stg_dcp__housing_ahft') }}
