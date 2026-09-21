{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- Borough is assigned by point-on-surface containment per the ETL spec's centroid
-- rule; point-on-surface keeps the probe inside concave shapes.
--
-- d.geom is bounded to its own assigned borough (shrunk 5 ft) before shoreline
-- clipping - see CSCL-DISTRICTS-03. Two distinct problems this catches:
--
-- 1. stg__neighborhood's raw boundary sometimes runs out to a legal/jurisdictional
--    line - e.g. the NY/NJ state line - that has no reason to coincide with any real
--    AtomicPolygon edge. Subtracting water then leaves a long, thin, un-clipped
--    "spur" reaching into open water, because there's no AtomicPolygon at all out
--    there to subtract (a genuine coverage void, not a data gap - confirmed by
--    checking whether any AtomicPolygon of any WATER_FLAG covers the spur, not just
--    whether water='1' does). Confirmed on SI12: a ~14,000 ft SHAPE_Length gap
--    against prod dropped to -9 ft once bounded to Staten Island's own extent.
-- 2. It also catches real errors in stg__neighborhood itself: QN99's raw polygon
--    extended all the way past Staten Island to Perth Amboy, NJ - a ~2.9B sq ft
--    polygon standing in for what should be a ~308M sq ft scatter of Queens parks
--    and cemeteries (prod's own QN99 is exactly that scatter). Bounding to Queens'
--    extent brings the area back in line with prod to within 0.24%.
--
-- The 5 ft shrink keeps the new intersection boundary from itself becoming a fresh
-- hairline mismatch against the true coastline in the much more common case where a
-- neighborhood's edge already sits right at its own borough's real edge.

WITH bounded AS (
    SELECT
        b.borocode::int AS "BoroCode",
        b.boroname AS "BoroName",
        b.fips AS "CountyFIPS",
        d.neighborhood_code AS "NTACode",
        npc.neighborhood_name AS "NTAName",
        st_intersection(d.geom, st_buffer(b.geom, -5)) AS geom
    FROM {{ ref('stg__neighborhood') }} AS d
    INNER JOIN {{ ref('stg__borough') }} AS b
        ON st_contains(b.geom, st_pointonsurface(d.geom))
    LEFT JOIN (
        SELECT DISTINCT
            neighborhood_code,
            neighborhood_name
        FROM {{ ref('stg__neighborhoodpumacodes') }}
    ) AS npc ON d.neighborhood_code = npc.neighborhood_code
),

clipped AS (
    SELECT
        "BoroCode",
        "BoroName",
        "CountyFIPS",
        "NTACode",
        "NTAName",
        {{ clipped_geom('bounded.geom') }} AS geom
    FROM bounded
    {{ clip_to_shoreline('bounded.geom') }}
)

SELECT
    *,
    st_perimeter(geom) AS "SHAPE_Length",
    st_area(geom) AS "SHAPE_Area"
FROM clipped
WHERE NOT st_isempty(geom)
