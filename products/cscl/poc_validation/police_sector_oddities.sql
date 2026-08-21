-- ThinLION police-sector diagnostic: point-in-polygon match vs. majority-area-overlap match
-- for a set of atomic polygons where the assigned sector looks off.
--
-- atomic_centroid_in_poly = the ST_CENTROID-with-ST_PointOnSurface-fallback representative
--   point used by the actual production join (thinlion_by_field_unformatted.sql).
-- police_poly1 = what production actually assigns (the beat polygon containing
--   atomic_centroid_in_poly).
-- police_poly2 = the beat polygon that covers the LARGEST share of the atomic polygon's area
--   (i.e. what you'd intuitively expect the sector to be).
--
-- All geometries are transformed from source SRID 2263 to 4326 for mapping.
-- Run from products/cscl with direnv loaded so run_sql_command hits the right db/schema.

WITH ap AS (
    SELECT
        atomicid,
        geom,
        CASE
            WHEN ST_WITHIN(ST_CENTROID(geom), geom) THEN ST_CENTROID(geom)
            WHEN ST_POINTONSURFACE(geom) IS NOT NULL THEN ST_POINTONSURFACE(geom)
            ELSE ST_CENTROID(geom)
        END AS rep_point
    FROM stg__atomicpolygons
    WHERE atomicid IN (
        '1000500093', '1000500130', '1000900012', '1024000006',
        '4064102160', '4071600162', '4107202045'
    )
),

point_match AS (
    SELECT
        ap.atomicid,
        ap.geom AS atomic_poly_geom,
        ap.rep_point,
        beat.sector AS point_sector,
        beat.geom AS point_beat_geom
    FROM ap
    LEFT JOIN stg__nypdbeat AS beat
        ON ST_WITHIN(ap.rep_point, beat.geom)
),

overlap_ranked AS (
    SELECT
        ap.atomicid,
        beat.sector,
        beat.geom AS beat_geom,
        ST_INTERSECTION(ap.geom, beat.geom) AS overlap_geom,
        ST_AREA(ST_INTERSECTION(ap.geom, beat.geom)) AS overlap_area,
        ROW_NUMBER() OVER (
            PARTITION BY ap.atomicid
            ORDER BY ST_AREA(ST_INTERSECTION(ap.geom, beat.geom)) DESC
        ) AS rn
    FROM ap
    JOIN stg__nypdbeat AS beat
        ON ST_INTERSECTS(ap.geom, beat.geom)
)

SELECT
    pm.atomicid,
    ST_TRANSFORM(pm.atomic_poly_geom, 4326) AS atomic_poly_geom,
    ST_TRANSFORM(pm.rep_point, 4326) AS atomic_centroid_in_poly,
    ST_TRANSFORM(pm.point_beat_geom, 4326) AS police_poly1,
    ST_TRANSFORM(ov.beat_geom, 4326) AS police_poly2,
    pm.point_sector AS police_poly1_sector,
    ov.sector AS police_poly2_sector,
    ov.overlap_area AS police_poly2_overlap_area
FROM point_match AS pm
LEFT JOIN overlap_ranked AS ov
    ON ov.atomicid = pm.atomicid AND ov.rn = 1
ORDER BY pm.atomicid;
