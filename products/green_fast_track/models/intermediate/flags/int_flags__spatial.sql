{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['bbl', 'variable_type']},
    ]
) }}

/*
We have a few monstrous polygons in our buffers, so a tiled approach greatly improves performance

First, a hex grid is generated for NYCs extent. 8000 as a size was roughly optimized for performance
This creates roughly a 13x14 grid of hexagons covering the city (175 hexagons total)

These hexagons are joined to the buffers table, creating one row per unique buffer and tiled hexagon

This buffer/hexagon table is then joined to pluto lots. hexagons geoms are used as first join criterion, then actual geom

Finally, rows are deduplicated based on bbl, variable_id, and variable_type
*/

WITH buffers AS (
    SELECT
        variable_type,
        variable_id,
        buffer,
        raw_geom
    FROM
        {{ ref('int_buffers__all') }}
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

buffers_hexes AS (
    SELECT
        b.*,
        hexes.geom
    FROM buffers AS b
    LEFT JOIN hexes ON ST_INTERSECTS(b.buffer, hexes.geom)
),

joined_hexes AS (
    SELECT DISTINCT ON (p.bbl, b.variable_type, b.variable_id)
        p.bbl,
        p.bbl_geom,
        b.variable_type,
        b.variable_id,
        b.raw_geom
    FROM buffers_hexes AS b INNER JOIN pluto AS p
        ON ST_INTERSECTS(b.geom, p.bbl_geom) AND ST_INTERSECTS(b.buffer, p.bbl_geom)
)

SELECT
    bbl,
    variable_type,
    variable_id,
    CASE
        WHEN variable_type = 'shadow_nat_resources' THEN 0
        ELSE ST_DISTANCE(bbl_geom, raw_geom)
    END AS distance
FROM joined_hexes
