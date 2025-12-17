{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['addresspointid']},
    ]
) }}

WITH address_points AS (
    SELECT * FROM {{ source("recipe_sources", "dcp_cscl_addresspoints") }}
),
street_names AS (
    SELECT * FROM {{ source("recipe_sources", "dcp_cscl_streetname") }}
    WHERE principal_flag = 'Y'
)
SELECT
    address_points.addresspointid,
    address_points.b7sc_actual,
    CASE
        WHEN street_names.snd_feature_type IN ('E', 'F') AND address_points.house_number_suffix IS NOT NULL
            THEN
                -- Last character is A, B, etc -> converted to 1, 2, etc
                (
                    10000 * COALESCE(ASCII(RIGHT(address_points.house_number_suffix, 1)) - 64, 0)
                    + address_points.house_number::INT
                )::TEXT
        WHEN address_points.hyphen_type = 'R'
            THEN TRIM(SPLIT_PART(address_points.house_number, '-', 1))
        ELSE address_points.house_number
    END AS house_number,
    address_points.house_number AS plain_house_number,
    street_names.snd_feature_type,
    address_points.geom
FROM address_points
LEFT JOIN street_names
    ON address_points.b7sc_actual = street_names.b7sc AND street_names.principal_flag = 'Y'
