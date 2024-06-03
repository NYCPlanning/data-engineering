{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['bbl', 'variable_type']},
    ]
) }}

WITH all_flags AS (
    SELECT
        bbl,
        flag_id_field_name,
        variable_type,
        variable_id,
        NULL::double precision AS distance
    FROM {{ ref('int_flags__zoning') }}
    UNION ALL
    SELECT
        bbl,
        flag_id_field_name,
        variable_type,
        variable_id,
        NULL::double precision AS distance
    FROM {{ ref('int_flags__edesignation') }}
    UNION ALL
    SELECT
        bbl,
        flag_id_field_name,
        variable_type,
        variable_id,
        NULL::double precision AS distance
    FROM {{ ref('int_flags__dob_natural_resources') }}
    UNION ALL
    SELECT
        bbl,
        flag_id_field_name,
        variable_type,
        variable_id,
        distance
    FROM {{ ref('int_flags__spatial') }}
)

SELECT * FROM all_flags
ORDER BY bbl ASC, flag_id_field_name ASC, variable_type ASC, variable_id ASC
