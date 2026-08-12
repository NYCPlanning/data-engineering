WITH pluto_raw AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_mappluto_wi') }}
),

final AS (
    SELECT
        bbl::bigint AS bbl,
        bct2020 AS boroct2020,
        -- stored CRS is labeled OGC:CRS84 (lon/lat, same axis order and values as
        -- the EPSG:4326 data we join against) - relabel rather than ST_Transform,
        -- which tries to fetch PROJ grid files over the network for exact
        -- geodetic transforms we don't need for a point-in-polygon test.
        ST_SETCRS(geom, 'EPSG:4326') AS geom
    FROM pluto_raw
)

SELECT * FROM final
