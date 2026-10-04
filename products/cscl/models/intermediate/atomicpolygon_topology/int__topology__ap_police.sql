{{ config(
    materialized='table',
    indexes=[{'columns': ['atomicid'], 'unique': True}]
) }}

-- Police geography of every AtomicPolygon - precinct, patrol borough and sector (NYPD
-- beat) - computed once here and shared, so ThinLION and anything else reading an AP's
-- police attributes can't disagree.
--
-- The rule reproduces prod's own per-AP assignment (verified against the published
-- ThinLION: 69,782 of 69,786 APs match on all three fields):
--   1. If the AP's centroid lies inside the AP (68,204 APs), use the polygon containing
--      the centroid - an exact match with prod for every one of them.
--   2. Otherwise - crescent, concave or multipart APs whose centroid falls outside them
--      (1,582 APs) - use the value holding the largest share of the AP's area (summed per
--      value: the beat layer has several polygons per sector). Here PostGIS's and the
--      legacy ETL's fallback points disagree, and majority area is what matches prod.
-- The 4 remaining mismatches are near-ties or slivers (e.g. 1024000006: 51% / 49%).
--
-- NYPD's polygons don't follow AP edges (their lines cut through hundreds of APs), which
-- is why the choice of rule matters at all. method / *_share record how each AP was
-- assigned. NULL where an AP touches no polygon of that layer.

{% set layers = [
    {'name': 'precinct', 'model': 'stg__nypdprecinct', 'id': 'precinct', 'out': 'police_precinct', 'globalid': 'nypdprecinct_globalid'},
    {'name': 'patrol_borough', 'model': 'stg__nypdpatrolborough', 'id': 'patrol_borough', 'out': 'patrol_borough', 'globalid': 'nypdpatrolborough_globalid'},
    {'name': 'sector', 'model': 'stg__nypdbeat', 'id': 'sector', 'out': 'police_sector', 'globalid': 'nypdbeat_globalid'},
] %}

WITH aps AS (
    SELECT
        atomicid,
        geom,
        st_centroid(geom) AS centroid,
        st_within(st_centroid(geom), geom) AS centroid_inside
    FROM {{ ref('stg__atomicpolygons') }}
),

{% for l in layers %}
    {{ l.name }}_at_centroid AS (
        SELECT DISTINCT ON (a.atomicid)
            a.atomicid,
            p.{{ l.id }} AS value,
            p.globalid
        FROM aps AS a
        INNER JOIN {{ ref(l.model) }} AS p ON st_intersects(p.geom, a.centroid)
        WHERE a.centroid_inside
        ORDER BY a.atomicid, p.globalid
    ),

    {{ l.name }}_overlap AS (
        -- only APs whose centroid falls outside them; an AP wholly inside one polygon skips
        -- the intersection
        SELECT
            a.atomicid,
            p.{{ l.id }} AS value,
            p.globalid,
            CASE
                WHEN st_covers(p.geom, a.geom) THEN st_area(a.geom)
                ELSE st_area(st_intersection(a.geom, p.geom))
            END AS overlap_sqft
        FROM aps AS a
        INNER JOIN {{ ref(l.model) }} AS p ON st_intersects(a.geom, p.geom)
        WHERE NOT a.centroid_inside
    ),

    {{ l.name }}_by_value AS (
        SELECT
            atomicid,
            value,
            sum(overlap_sqft) AS value_sqft,
            (array_agg(globalid ORDER BY overlap_sqft DESC))[1] AS globalid
        FROM {{ l.name }}_overlap
        WHERE overlap_sqft > 0
        GROUP BY atomicid, value
    ),

    {{ l.name }}_majority AS (
        SELECT DISTINCT ON (atomicid)
            atomicid,
            value,
            globalid,
            value_sqft / nullif(sum(value_sqft) OVER (PARTITION BY atomicid), 0) AS share
        FROM {{ l.name }}_by_value
        ORDER BY atomicid ASC, value_sqft DESC, value ASC
    ){{ ',' if not loop.last }}

{% endfor %}
SELECT
    a.atomicid,
    CASE WHEN a.centroid_inside THEN 'centroid' ELSE 'majority_area' END AS method,
{% for l in layers %}
    coalesce({{ l.name }}_at_centroid.value, {{ l.name }}_majority.value) AS {{ l.out }},
    round({{ l.name }}_majority.share::numeric, 4) AS {{ l.out }}_share,
    coalesce({{ l.name }}_at_centroid.globalid, {{ l.name }}_majority.globalid) AS {{ l.globalid }}
    {{- ',' if not loop.last }}
{% endfor %}
FROM aps AS a
{% for l in layers %}
    LEFT JOIN {{ l.name }}_at_centroid ON a.atomicid = {{ l.name }}_at_centroid.atomicid
    LEFT JOIN {{ l.name }}_majority ON a.atomicid = {{ l.name }}_majority.atomicid
{% endfor %}
