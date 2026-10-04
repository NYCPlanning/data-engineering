{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- Borough is assigned by point-on-surface containment per the ETL spec's centroid
-- rule; point-on-surface keeps the probe inside concave shapes.
--
-- Bounded to its own assigned borough (shrunk 0.001 ft) before shoreline clipping -
-- same fix as gdb_nynta2010/2020 and gdb_nypuma2010/2020 (see CSCL-DISTRICTS-03, chat
-- log 2026-10-03). SI01 North Shore's raw stg__cdta2020 boundary runs out across the
-- Kill Van Kull/Arthur Kill toward NJ - a genuine AtomicPolygon coverage void at the
-- jurisdictional line, same mechanism as the original SI12 case, just not yet fixed
-- for this layer.
--
-- 0.001 ft, not nynta's larger 5 ft: measured on SI01 across a 0.0001-5 ft sweep, the
-- fix is already ~99.99% complete at the smallest size tested (SHAPE_Length 153,715.6
-- vs. 153,706.7 ft at 5 ft - 0.006% apart) while losing 1,420x less real land (3.4 vs.
-- 4,830.7 sq ft) and zero interior-ring holes at every size tested. Unlike the
-- PUMA/census-tract dissolve case, the spurious boundary here sticks straight out into
-- open water, nowhere near the true coastline, so there's no real hairline-mismatch
-- risk to buy margin against with a bigger shrink - any nonzero shrink severs it
-- cleanly. 0.001 ft matches the overlay-precision gridSize used elsewhere in this
-- pipeline, not a value independently tuned for this layer.

WITH bounded AS (
    SELECT
        b.borocode::int AS "BoroCode",
        b.boroname AS "BoroName",
        b.fips AS "CountyFIPS",
        d.cdta_code AS "CDTA2020",
        cdta.cdta_name AS "CDTAName",
        cdta.cdta_type AS "CDTAType",
        st_intersection(d.geom, st_buffer(b.geom, -0.001)) AS geom
    FROM {{ ref('stg__cdta2020') }} AS d
    INNER JOIN {{ ref('stg__borough') }} AS b
        ON st_contains(b.geom, st_pointonsurface(d.geom))
    LEFT JOIN {{ ref('stg__cdtaequiv2020') }} AS cdta ON d.cdta_code = cdta.cdta_code
),

clipped AS (
    SELECT
        bounded."BoroCode",
        bounded."BoroName",
        bounded."CountyFIPS",
        bounded."CDTA2020",
        bounded."CDTAName",
        bounded."CDTAType",
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
