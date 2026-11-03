{{ config(
    materialized = 'table'
) }}

-- Underground (ROW_TYPE 1) subway and rail segments a centerline's coincident segment count
-- (LION L57) leaves out, CSCL-LION-14. Prod's L57 is the source COINCIDENT_SEG_COUNT less
-- one for each ROW_TYPE 1 subway or rail feature at the centerline's midpoint whose shape
-- Esri's IRelationalOperator.Equals finds equal to the centerline's
-- (CenterlineExtractFile.cs:2528-2610, GetCoincidentCLSegmentCount), with no floor, so a
-- count of 1 can come out 0.
--
-- subway_or_rail_count_prod: prod's rule. Equal shapes are each within
-- var segcount_coincident_tolerance_ft of the other, the rule gdb_lion's SegCount uses for
-- the same Equals test (int__lion_segcount).
-- subway_or_rail_count_fixed: ours before the var fix_coincident_seg_count: midpoints within
-- 1 ft, only on a centerline whose source count is above 1, so the count never goes below 1
-- (the spec's L57 is an integer above 0).
-- subway_or_rail_count: the one int__lion subtracts, prod's unless fix_coincident_seg_count.
WITH centerlines AS (
    SELECT
        segmentid,
        geom,
        midpoint
    FROM {{ ref('int__primary_segments') }}
    WHERE source_table = 'centerline'
),
prod_matches AS (
    SELECT
        centerlines.segmentid,
        COUNT(*) AS subway_or_rail_count
    FROM centerlines
    INNER JOIN {{ ref('int__underground_rail') }} AS rail
        ON ST_DWITHIN(rail.midpoint, centerlines.midpoint, 2.5)
    WHERE
        -- the subway/rail features prod reads are the ones at the centerline's midpoint
        ST_DWITHIN(rail.geom, centerlines.midpoint, {{ var('segcount_coincident_tolerance_ft') }})
        AND ST_COVERS(ST_BUFFER(rail.geom, {{ var('segcount_coincident_tolerance_ft') }}), centerlines.geom)
        AND ST_COVERS(ST_BUFFER(centerlines.geom, {{ var('segcount_coincident_tolerance_ft') }}), rail.geom)
    GROUP BY centerlines.segmentid
),
fixed_matches AS (
    SELECT
        all_segments_id AS segmentid,
        COUNT(*) AS subway_or_rail_count
    FROM {{ ref('int__centerline_coincident_subway_or_rail_matches') }}
    WHERE starting_coincident_seg_count > 1 AND distance < 1
    GROUP BY all_segments_id
)
SELECT
    COALESCE(prod_matches.segmentid, fixed_matches.segmentid) AS segmentid,
    COALESCE(prod_matches.subway_or_rail_count, 0) AS subway_or_rail_count_prod,
    COALESCE(fixed_matches.subway_or_rail_count, 0) AS subway_or_rail_count_fixed,
    COALESCE(
        {% if var('fix_coincident_seg_count') -%}
            fixed_matches.subway_or_rail_count
        {%- else -%}
            prod_matches.subway_or_rail_count
        {%- endif %},
        0
    ) AS subway_or_rail_count
FROM prod_matches
FULL JOIN fixed_matches ON prod_matches.segmentid = fixed_matches.segmentid
