{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- Police precincts: the NYPD source precinct polygons clipped to land (int__boundary__pp;
-- see that model for why this layer isn't built from the topology). Columns and types
-- match the published layer.

SELECT
    entity_id AS "Precinct",
    geom,
    st_perimeter(geom) AS "SHAPE_Length",
    st_area(geom) AS "SHAPE_Area"
FROM {{ ref('int__boundary__pp') }}
