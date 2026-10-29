{{ config(
    materialized = 'table'
) }}

-- Curve flag (LION L49) of the segments that aren't centerlines: shoreline, subway, rail,
-- non-street features and their proto-segments (a centerline's comes from its CURVE
-- field, int__centerline_curve). Prod writes I when the source shape has more than two
-- points (ShorelineExtractFile.cs:422, GetHasShapePoints; ETL spec 2.3.3 "shape
-- points"). A circular arc is two points in the source (its ends; the mid control point
-- isn't a vertex), so a segment that is one arc end to end stays blank in prod: CSCL-LION-03.
-- The fix (var fix_single_arc_curve_flag) counts the points of the densified line instead,
-- so the arc gets I. A proto-segment counts its parent's shape, as prod does.
WITH segments AS (
    SELECT
        globalid,
        source_table,
        source_class_rank,
        source_objectid,
        geom,
        raw_geom
    FROM {{ ref("int__segments") }}
    WHERE feature_type <> 'centerline'
),
-- each arc replaces its densified run of vertices v0..v1 with its two ends
arc_interior_points AS (
    {%- for rank, layer in [
        (1, 'centerline'), (2, 'nonstreetfeatures'), (3, 'rail'), (4, 'shoreline'), (5, 'subway')
    ] %}
        SELECT
            {{ rank }} AS source_class_rank,
            ogc_fid,
            SUM(v1 - v0 - 1) AS interior_points
        FROM {{ source("recipe_sources", "dcp_cscl_" ~ layer ~ "_arcs") }}
        GROUP BY ogc_fid
        {% if not loop.last -%}
            UNION ALL
        {%- endif %}
    {%- endfor %}
),
counted AS (
    SELECT
        segments.globalid,
        segments.source_table,
        ST_NPOINTS(segments.raw_geom) - COALESCE(arc_interior_points.interior_points, 0) AS source_point_count,
        ST_NUMPOINTS(segments.geom) AS densified_point_count
    FROM segments
    -- source_class_rank and source_objectid are the parent's for a proto-segment
    LEFT JOIN arc_interior_points
        ON
            segments.source_class_rank = arc_interior_points.source_class_rank
            AND segments.source_objectid = arc_interior_points.ogc_fid
)
SELECT
    globalid,
    source_table,
    source_point_count,
    densified_point_count,
    CASE WHEN source_point_count > 2 THEN 'I' END AS curve_flag_prod,
    CASE WHEN densified_point_count > 2 THEN 'I' END AS curve_flag_fixed,
    {% if var('fix_single_arc_curve_flag') -%}
        curve_flag_fixed
    {%- else -%}
        curve_flag_prod
    {%- endif %} AS curve_flag
FROM counted
