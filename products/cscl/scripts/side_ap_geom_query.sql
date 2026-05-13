-- Query to visualize side_ap discrepancies with geometries
-- Transforms all geometries to EPSG:4326 (WGS84) for mapping

WITH side_ap_diffs AS (
    SELECT
        comparison_id,
        changes -> 'side_ap' ->> 'old' AS old_side_ap,
        changes -> 'side_ap' ->> 'new' AS new_side_ap
    FROM ar_cscl_diffs1.qa__diffs_all
    WHERE diff_group = 'side_ap'
),
point_locations AS (
    SELECT
        s._saf_key,
        d.old_side_ap,
        d.new_side_ap,
        s.boroughcode,
        s.saftype,
        s.segmentid,
        s.x_coord,
        s.y_coord,
        ST_SETSRID(ST_MAKEPOINT(s.x_coord::float, s.y_coord::float), 2263) AS point_geom_2263
    FROM ar_cscl_diffs1.saf_abcegnpx_roadbed_by_field AS s
    INNER JOIN side_ap_diffs AS d ON s._saf_key = d.comparison_id
),
with_atomic_polygons AS (
    SELECT
        p._saf_key,
        p.old_side_ap,
        p.new_side_ap,
        p.boroughcode,
        p.saftype,
        p.segmentid,
        p.x_coord,
        p.y_coord,
        p.point_geom_2263,
        -- Actual atomic polygon containing the point (may not match side_ap due to multiple polygons with same suffix)
        ap_actual.atomicid AS actual_atomicid,
        RIGHT(ap_actual.atomicid, 3) AS actual_side_ap,
        ap_actual.censustract_2020_basic || LPAD(ap_actual.censustract_2020_suffix::text, 2, '0') AS actual_ct2020,
        ap_actual.censusblock_2020_basic AS actual_cb2020,
        ap_actual.geom AS actual_ap_geom,
        -- Check if side_ap matches
        COALESCE(RIGHT(ap_actual.atomicid, 3) = p.new_side_ap, FALSE) AS side_ap_matches
    FROM point_locations AS p
    LEFT JOIN ar_cscl_diffs1.stg__atomicpolygons AS ap_actual
        ON ST_CONTAINS(ap_actual.geom, p.point_geom_2263)
)
SELECT
    wap._saf_key,
    wap.old_side_ap,
    wap.new_side_ap,
    wap.actual_side_ap,
    wap.side_ap_matches,
    wap.boroughcode,
    wap.saftype,
    wap.segmentid,
    wap.actual_atomicid,
    wap.actual_ct2020,
    wap.actual_cb2020,
    -- Point geometry in 4326 (PostGIS geometry type)
    ST_TRANSFORM(wap.point_geom_2263, 4326) AS point_geom,
    -- Atomic polygon geometry in 4326 (PostGIS geometry type)
    ST_TRANSFORM(wap.actual_ap_geom, 4326) AS atomic_polygon_geom,
    -- Segment geometry in 4326 (PostGIS geometry type)
    ST_TRANSFORM(seg.geom, 4326) AS segment_geom
FROM with_atomic_polygons AS wap
LEFT JOIN ar_cscl_diffs1.int__primary_segments AS seg
    ON wap.segmentid = seg.segmentid::text
ORDER BY wap._saf_key;
