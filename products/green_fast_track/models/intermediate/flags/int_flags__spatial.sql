{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['bbl', 'variable_type']},
    ]
) }}

/*
We have a few monstrous polygons in our variable geometries, so a tiled approach greatly improves performance

First, a hex grid is generated for NYCs extent. 8000 as a size was roughly optimized for performance
This creates roughly a 13x14 grid of hexagons covering the city (175 hexagons total)

These hexagons are joined to the variable geometries table, creating one row per unique variable geometry and tiled hexagon

This variable geoemtry/hexagon table is then joined to pluto lots. hexagons geoms are used as first join criterion, then actual geom

Finally, rows are deduplicated based on bbl, variable_id, and variable_type
*/

WITH variable_geoms AS (
    SELECT
        variable_type,
        variable_id,
        raw_geom,
        variable_geom
    FROM
        {{ ref('int_spatial__all') }}
),

pluto AS (
    SELECT
        bbl,
        geom AS bbl_geom
    FROM {{ ref('stg__pluto') }}
),

hexes AS (
    {% set nyc_rel = ref('stg__nyc_boundary') %}
    SELECT * FROM ST_HEXAGONGRID(
        8000,
        ST_SETSRID(ST_ESTIMATEDEXTENT('{{ nyc_rel.schema }}', '{{ nyc_rel.identifier }}', 'geom'), 2263)
    )
),

variable_geom_hexes AS (
    SELECT
        b.*,
        hexes.geom
    FROM variable_geoms AS b
    LEFT JOIN hexes ON ST_INTERSECTS(b.variable_geom, hexes.geom)
),

joined_hexes AS (
    SELECT DISTINCT ON (p.bbl, b.variable_type, b.variable_id)
        p.bbl,
        p.bbl_geom,
        b.variable_type,
        b.variable_id,
        b.raw_geom
    FROM variable_geom_hexes AS b INNER JOIN pluto AS p
        ON ST_INTERSECTS(b.geom, p.bbl_geom) AND ST_INTERSECTS(b.variable_geom, p.bbl_geom)
)

SELECT
    bbl,
    variable_type,
    variable_id,
    CASE
        WHEN
            variable_type IN (
                'archaeological_areas', 'shadow_open_spaces', 'shadow_nat_resources', 'shadow_hist_resources'
            )
            THEN 0
        ELSE ST_DISTANCE(bbl_geom, raw_geom)
    END AS distance
FROM joined_hexes
ORDER BY bbl ASC, variable_type ASC, variable_id ASC
