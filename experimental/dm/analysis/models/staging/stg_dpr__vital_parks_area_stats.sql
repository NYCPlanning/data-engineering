-- one row per area. Percentages are read as numbers, and Data Unavailable as null.
{% set elements = [
    'Active Recreation Areas', 'Community Gardens', 'Cooling Features', 'Dog Runs', 'Forever Wild Natural Areas',
    'Greenways', 'Hiking Trails', 'Nature Centers', 'New or Renovated Parks', 'Park Partner Groups',
    'Parks Enforcement Patrol', 'Parks With High Condition Scores', 'Playgrounds', 'Public Events',
    'Public Restrooms', 'Recreation Centers', 'Street Trees', 'Swimming Facilities', 'Tree Canopy Cover',
    'Volunteer Events', 'Waterfronts',
] %}
SELECT
    "Area Type" AS area_type,
    "Name/Number" AS area_name,
    "Population"::INTEGER AS population,
    {%- for element in elements %}
    {%- set column = element.lower().replace(' ', '_') %}
    rtrim(nullif("{{ element }}", 'Data Unavailable'), '%')::DOUBLE AS {{ column }}_pct,
    rtrim(nullif("{{ element }} Future", 'Data Unavailable'), '%')::DOUBLE AS {{ column }}_future_pct{{ ',' if not loop.last }}
    {%- endfor %}
FROM {{ source('sources', 'dpr_vital_parks_area_stats') }}
