{{ config(
    materialized='table',
    indexes=[{'columns': ['entity_id'], 'unique': True}, {'columns': ['geom'], 'type': 'gist'}]
) }}

-- Police precinct boundaries: the NYPD source precinct polygons clipped to land - not
-- built from the AtomicPolygon topology, since NYPD's lines don't follow AP edges (they
-- cut through hundreds of APs; see int__topology__ap_police). Land is the union of the
-- topology-built borough boundaries (int__boundary__bb), so the shoreline matches every
-- other layer. Clipping a line that runs almost along the shoreline leaves hairline
-- slivers; parts and holes under 100 sq ft are dropped (every sliver observed is under
-- 1 sq ft; real pieces are far larger).

WITH land AS (
    SELECT st_union(geom) AS geom
    FROM {{ ref('int__boundary__bb') }}
),

clipped AS (
    SELECT
        p.precinct::smallint AS precinct,
        (st_dump(st_collectionextract(st_intersection(p.geom, land.geom), 3))).geom AS part
    FROM {{ ref('stg__nypdprecinct') }} AS p
    CROSS JOIN land
),

cleaned AS (
    SELECT
        c.precinct,
        st_makepolygon(
            st_exteriorring(c.part),
            coalesce(
                (
                    SELECT array_agg(st_interiorringn(c.part, n))
                    FROM generate_series(1, st_numinteriorrings(c.part)) AS n
                    WHERE st_area(st_makepolygon(st_interiorringn(c.part, n))) >= 100
                ),
                ARRAY[]::geometry[]
            )
        ) AS geom
    FROM clipped AS c
    WHERE st_area(c.part) >= 100
)

SELECT
    precinct AS entity_id,
    st_multi(st_union(geom)) AS geom
FROM cleaned
GROUP BY precinct
