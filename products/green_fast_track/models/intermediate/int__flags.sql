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
FROM {{ ref('int__zoning_flags') }}
UNION ALL
SELECT
    bbl,
    variable_type,
    variable_id,
    NULL::double precision AS distance
FROM {{ ref('int__edesignation_flags') }}
UNION ALL
SELECT
    bbl,
    variable_type,
    variable_id,
    distance
FROM {{ ref('int__spatial_flags') }}
