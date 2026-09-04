{{ config(
    materialized = 'table'
) }}

/*
Postgis's version of this model tiled the city into a hex grid (ST_HexagonGrid +
ST_EstimatedExtent) before joining, since a few monstrous variable-geometry polygons made a
direct spatial join slow. Duckdb has no equivalent to either function, and its spatial join
already does its own bounding-box pruning, so this just joins variable geometries to pluto
lots directly.

Postgis's version deduplicated on bbl, variable_id, variable_type, and flag_id_field_name after
the join with DISTINCT ON, with no explicit tie-break (an arbitrary but deterministic-per-plan
pick). The dedup is still needed here -- e.g. hundreds of individual `nyc_historic_buildings`
landmark records share a single district name as their variable_id, so multiple raw_geom rows
can match the same (bbl, variable_id). Deduplicating by hashing/comparing full geometry blobs
across the ~9M joined rows (via DISTINCT, or a ROW_NUMBER/QUALIFY window requiring the whole
partition materialized) blew through memory. A GROUP BY + MIN(distance) aggregate needs none of
that -- every output column downstream is either a group key or this aggregate, so a streaming
hash aggregate produces the identical result set (and MIN is a better tie-break than postgis's
arbitrary pick: it keeps the closest duplicate).
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
)

SELECT
    p.bbl,
    b.flag_id_field_name,
    b.variable_type,
    b.variable_id,
    MIN(
        CASE
            WHEN
                -- don't calculate distance for spatial flags with a single city-wide geometry
                b.flag_id_field_name IN (
                    'archaeological_area', 'shadow_open_spaces', 'shadow_nat_resources', 'shadow_hist_resources'
                )
                THEN 0
            ELSE ST_DISTANCE(p.bbl_geom, b.raw_geom)
        END
    ) AS distance
FROM variable_geoms AS b INNER JOIN pluto AS p
    ON ST_INTERSECTS(b.variable_geom, p.bbl_geom)
GROUP BY p.bbl, b.flag_id_field_name, b.variable_type, b.variable_id
ORDER BY bbl ASC, flag_id_field_name ASC, variable_type ASC, variable_id ASC
