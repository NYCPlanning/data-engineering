{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['bbl', 'variable_type']},
    ]
) }}

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
