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
        variable_type,
        variable_id,
        NULL::double precision AS distance
    FROM {{ ref('int_flags__zoning') }}
    UNION ALL
    SELECT
        bbl,
        variable_type,
        variable_id,
        NULL::double precision AS distance
    FROM {{ ref('int_flags__edesignation') }}
    UNION ALL
    SELECT
        bbl,
        variable_type,
        variable_id,
        distance
    FROM {{ ref('int_flags__spatial') }}
),

all_flags AS (
    SELECT
        bbl,
        variables.flag_id_field_name,
        all_variable_ids.variable_type,
        variable_id,
        distance
    FROM all_variable_ids LEFT JOIN variables
        ON all_variable_ids.variable_type = variables.variable_type
)

SELECT * FROM all_flags
ORDER BY bbl ASC, flag_id_field_name ASC, variable_type ASC, variable_id ASC
