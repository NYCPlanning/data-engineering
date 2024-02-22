{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['bbl', 'variable_type']},
    ]
) }}

--todo - add in 'int__zoning_flags'
--UNION ALL
SELECT
    bbl,
    variable_type,
    variable_id,
    NULL AS distance
FROM {{ ref('int__edesignation_flags') }}
UNION ALL
SELECT
    bbl,
    variable_type,
    variable_id,
    distance
FROM {{ ref('int__spatial_flags') }}
