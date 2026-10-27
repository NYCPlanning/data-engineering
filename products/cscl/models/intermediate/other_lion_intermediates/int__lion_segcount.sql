{{ config(materialized='table') }}

-- One row per LION gdb segment with its uncapped record count, for gdb_lion's SegCount.

WITH lion AS (
    SELECT * FROM {{ ref('int__lion') }}
    WHERE include_in_bytes_lion
),
saf_replicants AS (
    SELECT * FROM {{ ref('int__saf_replicant_fields') }}
),

-- SegCount (legacy GetCoincidentSegmentCount): the number of output records for the
-- segment (SAF replicants and continuous-parity copies included), plus one for each
-- AltSegmentData SAF record that isn't written out as a replicant (e.g. type Z) and for
-- each CommonPlace G/N/X record without a B7SC, plus
-- one for each other segment of the same feature class (centerline with centerline,
-- rail with rail, ...) with equal geometry. gdb_lion caps it at 9.
-- Legacy tests equality within the source feature classes' XY tolerance (0.0002 ft, on a
-- 0.0001 ft resolution grid), so equal here means the same two nodes and each geometry
-- within var segcount_coincident_tolerance_ft of the other. Densified true curves differ
-- by float noise, so an exact geometry match misses some.
segment_geoms AS (
    SELECT
        segmentid,
        source_table,
        any_value(least(from_nodeid, to_nodeid)) AS node_a,
        any_value(greatest(from_nodeid, to_nodeid)) AS node_b,
        any_value(geom) AS geom
    FROM {{ ref('int__lion') }}
    WHERE
        source_table = 'centerline'
        OR (include_in_bytes_lion AND source_table IN ('rail', 'subway', 'nonstreetfeatures'))
    GROUP BY segmentid, source_table
),

coincident_segment_counts AS (
    SELECT
        segment.segmentid,
        count(DISTINCT other.segmentid) AS n_coincident
    FROM segment_geoms AS segment
    INNER JOIN segment_geoms AS other
        ON
            segment.source_table = other.source_table
            AND segment.node_a = other.node_a
            AND segment.node_b = other.node_b
            AND segment.segmentid <> other.segmentid
    WHERE
        st_covers(st_buffer(other.geom, {{ var('segcount_coincident_tolerance_ft') }}), segment.geom)
        AND st_covers(st_buffer(segment.geom, {{ var('segcount_coincident_tolerance_ft') }}), other.geom)
    GROUP BY segment.segmentid
),

segment_row_counts AS (
    SELECT
        per_record.segmentid,
        sum(per_record.n_rows) + coalesce(max(coincident_segment_counts.n_coincident), 0) AS n_records
    FROM (
        SELECT
            segmentid,
            CASE WHEN continuous_parity_flag IN ('L', 'R') THEN 2 ELSE 1 END AS n_rows
        FROM lion
        UNION ALL
        SELECT
            lion.segmentid,
            CASE WHEN saf_replicants.parity_side IS NOT NULL THEN 2 ELSE 1 END AS n_rows
        FROM lion
        INNER JOIN saf_replicants ON lion.lionkey = saf_replicants.lionkey
        UNION ALL
        SELECT
            lion.segmentid,
            1 AS n_rows
        FROM lion
        INNER JOIN {{ ref('int__saf_segments') }} AS saf_segments
            ON lion.lionkey = saf_segments.segment_lionkey
        WHERE
            saf_segments.saf_source_table = 'altsegmentdata'
            AND NOT EXISTS (
                SELECT 1 FROM saf_replicants
                WHERE
                    saf_replicants.saf_globalid = saf_segments.saf_globalid
                    AND saf_replicants.lionkey = saf_segments.segment_lionkey
            )
        UNION ALL
        -- CommonPlace G/N/X records without a B7SC are never written (int__saf_segments
        -- drops them), but legacy counts every SAF record, written or not
        SELECT
            lion.segmentid,
            1 AS n_rows
        FROM lion
        INNER JOIN {{ source('recipe_sources', 'dcp_cscl_commonplace_gdb') }} AS commonplace
            ON lion.segmentid = commonplace.segmentid
        WHERE
            lion.source_table IN ('centerline', 'shoreline')
            AND commonplace.saftype IN ('G', 'N', 'X')
            AND commonplace.b7sc IS NULL
    ) AS per_record
    LEFT JOIN coincident_segment_counts ON per_record.segmentid = coincident_segment_counts.segmentid
    GROUP BY per_record.segmentid
)

SELECT * FROM segment_row_counts
