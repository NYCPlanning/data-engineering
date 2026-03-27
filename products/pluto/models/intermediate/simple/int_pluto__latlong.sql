{{
    config(
        materialized='table',
        indexes=[{'columns': ['bbl'], 'unique': True}],
        tags=['pluto_enrichment']
    )
}}

-- Calculate latitude and longitude from x/y coordinates
-- Transform from State Plane (SRID 2263) to WGS84 (SRID 4326)
-- Also create centroid geometry point

SELECT
    bbl,
    ST_Y(
        ST_TRANSFORM(
            ST_SETSRID(
                ST_MAKEPOINT(xcoord::double precision, ycoord::double precision),
                2263
            ),
            4326
        )
    ) AS latitude,
    ST_X(
        ST_TRANSFORM(
            ST_SETSRID(
                ST_MAKEPOINT(xcoord::double precision, ycoord::double precision),
                2263
            ),
            4326
        )
    ) AS longitude,
    ST_SETSRID(
        ST_MAKEPOINT(
            ST_X(
                ST_TRANSFORM(
                    ST_SETSRID(
                        ST_MAKEPOINT(xcoord::double precision, ycoord::double precision),
                        2263
                    ),
                    4326
                )
            )::double precision,
            ST_Y(
                ST_TRANSFORM(
                    ST_SETSRID(
                        ST_MAKEPOINT(xcoord::double precision, ycoord::double precision),
                        2263
                    ),
                    4326
                )
            )::double precision
        ),
        4326
    ) AS centroid
FROM {{ target.schema }}.pluto
WHERE xcoord IS NOT NULL
