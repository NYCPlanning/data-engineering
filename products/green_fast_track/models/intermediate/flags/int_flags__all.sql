{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['bbl', 'variable_type']},
    ]
) }}

WITH variables AS (
    SELECT * FROM {{ ref('variables') }}
),

all_variable_ids AS (
    SELECT
        bbl,
        flag_variable_type,
        variable_type,
        variable_id,
        NULL::double precision AS distance
    FROM {{ ref('int_flags__zoning') }}
    UNION ALL
    SELECT
        bbl,
        flag_variable_type,
        variable_type,
        variable_id,
        NULL::double precision AS distance
    FROM {{ ref('int_flags__edesignation') }}
    UNION ALL
    SELECT
        bbl,
        flag_variable_type,
        variable_type,
        variable_id,
        distance
    FROM {{ ref('int_flags__spatial') }}
),

all_flags AS (
    SELECT
        all_variable_ids.bbl,
        all_variable_ids.flag_variable_type,
        variables.flag_id_field_name,
        all_variable_ids.variable_type,
        all_variable_ids.variable_id,
        all_variable_ids.distance
    FROM all_variable_ids LEFT JOIN variables
        ON all_variable_ids.flag_variable_type = variables.flag_variable_type
)

SELECT * FROM all_flags
ORDER BY bbl ASC, flag_id_field_name ASC, variable_type ASC, variable_id ASC
