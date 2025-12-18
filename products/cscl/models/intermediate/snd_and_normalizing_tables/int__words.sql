{{ config(
    materialized='table',
    indexes=[
      {'columns': ['word']}
    ]
) }}

/*
This model is currently only referenced in int__exception, where it maybe is not actually needed.
See comment in that model
*/

{%- set fields = [
    'full_name',
    'standard_abbreviation',
    'shortest_abbreviation',
    'other_abbreviation',
    'other_abbreviation_2',
    'other_abbreviation_3',
    'other_abbreviation_4',
    'other_abbreviation_5',
    'other_abbreviation_6',
    'other_abbreviation_7',
    'other_abbreviation_8',
    'other_abbreviation_9',
    'other_abbreviation_10'
] -%}
WITH last_word AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_cscl_lastword') }}
),
universal_word AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_cscl_universalword') }}
)
{%- for field in fields %}
    SELECT {{ field }} AS word FROM last_word
    UNION
    SELECT {{ field }} AS word FROM universal_word
    {% if not loop.last -%}UNION{% endif %}
{%- endfor %}
