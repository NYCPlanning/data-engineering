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

Finally, rows are deduplicated based on bbl, variable_id, variable_type, and flag_id_field_name
*/

WITH variable_geoms AS (
    SELECT
        flag_id_field_name,
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
    SELECT * FROM ST_HexagonGrid(
        8000,
        ST_SetSRID(ST_EstimatedExtent('{{ nyc_rel.schema }}', '{{ nyc_rel.identifier }}', 'geom'), 2263)
    )
),

variable_geom_hexes AS (
    SELECT
        b.*,
        hexes.geom
    FROM variable_geoms AS b
    LEFT JOIN hexes ON ST_Intersects(b.variable_geom, hexes.geom)
),

joined_hexes AS (
    SELECT DISTINCT ON (p.bbl, b.flag_id_field_name, b.variable_type, b.variable_id)
        p.bbl,
        p.bbl_geom,
        b.flag_id_field_name,
        b.variable_type,
        b.variable_id,
        b.raw_geom
    FROM variable_geom_hexes AS b INNER JOIN pluto AS p
        ON ST_Intersects(b.geom, p.bbl_geom) AND ST_Intersects(b.variable_geom, p.bbl_geom)
)

SELECT
    bbl,
    flag_id_field_name,
    variable_type,
    variable_id,
    CASE
        WHEN
            -- don't calculate distance for spatial flags with a single city-wide geometry
            flag_id_field_name IN (
                'archaeological_area', 'shadow_open_spaces', 'shadow_nat_resources', 'shadow_hist_resources'
            )
            THEN 0
        ELSE ST_Distance(bbl_geom, raw_geom)
    END AS distance
FROM joined_hexes
ORDER BY bbl ASC, flag_id_field_name ASC, variable_type ASC, variable_id ASC
