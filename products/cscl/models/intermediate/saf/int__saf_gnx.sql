{{ config(
    materialized = 'table'
) }}

WITH saf AS (
    SELECT * FROM {{ ref('int__saf_segments') }}
    -- prod's SAF dat drops SECURITY_LEVEL 3 CommonPlaces (its LION gdb keeps them)
    WHERE security_level <> 3
),
commonplace AS (
    SELECT * FROM {{ source("recipe_sources", "dcp_cscl_commonplace_gdb") }}
),
address_points AS (
    SELECT * FROM {{ ref('int__address_points') }}
),
atomic_polygons AS (
    SELECT * FROM {{ ref('stg__atomicpolygons') }}
),
-- The AtomicPolygon each place point falls in, using the source FGDB's XY tolerance
-- (0.0002 ft) as prod's Esri spatial filter does: a point within the tolerance of an AP
-- boundary is on that boundary, so prod's Within finds nothing, its Intersects fallback
-- returns every AP within the tolerance, and it takes the first (lowest OBJECTID). So:
-- the AP containing the point farther than the tolerance from its boundary, else the
-- lowest ogc_fid among APs within the tolerance (CSCL-SAF-02). ogc_fid is used only for
-- this ordering: its order follows the source FGDB's OBJECTID order because GDAL reads
-- OpenFileGDB layers in FID order.
point_atomic_polygon AS (
    SELECT
        commonplace.globalid,
        atomic_polygons.atomicid,
        atomic_polygons.borocode,
        atomic_polygons.censustract_2020_basic,
        atomic_polygons.censustract_2020_suffix
    FROM commonplace
    INNER JOIN atomic_polygons ON ST_DWITHIN(atomic_polygons.geom, commonplace.geom, 0.0002)
    WHERE commonplace.globalid IN (SELECT saf_globalid FROM saf)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY commonplace.globalid
        ORDER BY
            ST_CONTAINS(atomic_polygons.geom, commonplace.geom)
            AND ST_DISTANCE(ST_BOUNDARY(atomic_polygons.geom), commonplace.geom) > 0.0002 DESC,
            atomic_polygons.ogc_fid
    ) = 1
),
feature_names AS (
    SELECT * FROM {{ ref('stg__facecode_and_featurename_principal') }}
),
-- prod leaves out a place whose SEGMENTID matches no segment, and logs it (CSCL-LOG-SAF-25)
orphans AS (
    SELECT globalid FROM {{ ref('int__saf_orphan_segmentids') }}
),
-- nor a place on a segment whose proto doesn't match it (CSCL-LOG-SAF-26, -27)
proto_mismatch AS (
    SELECT globalid FROM {{ ref('int__saf_commonplace_proto') }}
)
SELECT
    feature_names.place_name_sort_order AS place_name,
    saf.boroughcode,
    SUBSTRING(saf.segment_lionkey, 2, 4) AS face_code,
    SUBSTRING(saf.segment_lionkey, 6, 5) AS segment_seqnum,
    CASE
        WHEN commonplace.sosindicator = '1' THEN 'L'
        WHEN commonplace.sosindicator = '2' THEN 'R'
    END AS sos_indicator,
    LEFT(commonplace.b7sc, 6) AS b5sc,
    '0' AS l_low_hn,
    COALESCE(CASE WHEN commonplace.sosindicator = '1' THEN address_points.house_number END, '0') AS l_high_hn,
    '0' AS r_low_hn,
    COALESCE(CASE WHEN commonplace.sosindicator = '2' THEN address_points.house_number END, '0') AS r_high_hn,
    commonplace.saftype,
    RIGHT(commonplace.b7sc, 2) AS lgc1,
    NULL AS lgc2,
    NULL AS lgc3,
    NULL AS lgc4,
    '1' AS boe_lgc_pointer,
    saf.segment_type,
    saf.segmentid,
    ROUND_EVEN(ST_X(commonplace.geom), 0)::INT AS x_coord,
    ROUND_EVEN(ST_Y(commonplace.geom), 0)::INT AS y_coord,
    point_atomic_polygon.borocode AS side_borough_code,
    point_atomic_polygon.censustract_2020_basic AS side_ct2020_basic,
    point_atomic_polygon.censustract_2020_suffix AS side_ct2020_suffix,
    RIGHT(point_atomic_polygon.atomicid, 3) AS side_ap,
    saf.saf_globalid,
    saf.saf_source_table,
    commonplace.placeid,
    commonplace.primaryaddresspointid,
    saf.generic,
    saf.roadbed
FROM saf
LEFT JOIN commonplace ON saf.saf_globalid = commonplace.globalid
LEFT JOIN address_points ON commonplace.primaryaddresspointid = address_points.addresspointid
LEFT JOIN feature_names ON commonplace.b7sc = feature_names.b7sc
LEFT JOIN point_atomic_polygon ON commonplace.globalid = point_atomic_polygon.globalid
ANTI JOIN orphans ON saf.saf_globalid = orphans.globalid
ANTI JOIN proto_mismatch ON saf.saf_globalid = proto_mismatch.globalid
WHERE
    saf.saftype IN ('G', 'N', 'X')
    -- legacy drops a record with no place name
    AND feature_names.place_name_sort_order IS NOT NULL
