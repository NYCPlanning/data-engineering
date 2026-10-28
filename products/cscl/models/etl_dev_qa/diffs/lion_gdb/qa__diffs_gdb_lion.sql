{{
  config(
    materialized='table',
    enabled=var('qa_gdb_lion_diffs'),
    tags=['on_demand', 'diffs', 'diffs_lion_gdb']
  )
}}

-- Compressed diff view of the LION geodatabase `lion` layer: one row per _gdb_lion_key,
-- with status modified / only_in_legacy / only_in_build and the changed fields in jsonb.
-- Keys and the house-number fallback pairing are built in qa_int__*_gdb_lion_keyed.
--
-- Each changed field gets a cause label and an "explained" flag; a modified row is
-- accounted for when every changed field is explained. diff_group names the single
-- cause behind the row (or the single cause left unexplained).

WITH base_diffs AS (
  {{ generate_diff_summary(
      old_relation=ref('qa_int__prod_gdb_lion_keyed'),
      new_relation=ref('qa_int__build_gdb_lion_keyed'),
      primary_key='_gdb_lion_key',
      exclude_columns=['_gdb_lion_fk', '_unmatched', '_rank', '_feature_type', '_source_table'],
      output_file_id='gdb_lion',
      build_table_name='qa_int__build_gdb_lion_keyed',
      production_table_name='qa_int__prod_gdb_lion_keyed'
  ) }}
),
-- CSCL-LION-16: segments whose left or right AP comes from the topology and differs from
-- the 2 ft probe's (prod's method). A difference only with fix_lion_lr_from_topology on:
-- off (the default) this is empty, so a side-AP diff is unaccounted.
topology_lr_segments AS (
    SELECT DISTINCT lpad(segmentid::text, 7, '0') AS segmentid
    FROM {{ ref('int__segment_atomicpolygons') }}
    WHERE
        {{ 'TRUE' if var('fix_lion_lr_from_topology') else 'FALSE' }}
        AND (
            left_atomicid IS DISTINCT FROM probe_left_atomicid
            OR right_atomicid IS DISTINCT FROM probe_right_atomicid
        )
),
-- CSCL-LION-14 (gdb SegCount): centerlines with another centerline on the same two nodes
-- whose geometry is within 0.0005 ft of theirs. gdb_lion counts a pair as coincident within
-- var segcount_coincident_tolerance_ft (0.0003 ft); prod's Esri shape-equality test counts
-- some pairs just past that and not others, so no tolerance reproduces it (inv-2cy.55).
centerline_geoms AS (
    SELECT
        lpad(segmentid::text, 7, '0') AS segmentid,
        any_value(least(from_nodeid, to_nodeid)) AS node_a,
        any_value(greatest(from_nodeid, to_nodeid)) AS node_b,
        any_value(geom) AS geom
    FROM {{ ref('int__lion') }}
    WHERE source_table = 'centerline'
    GROUP BY segmentid
),
near_coincident_centerlines AS (
    SELECT DISTINCT segment.segmentid
    FROM centerline_geoms AS segment
    INNER JOIN centerline_geoms AS other
        ON
            segment.node_a = other.node_a
            AND segment.node_b = other.node_b
            AND segment.segmentid <> other.segmentid
    WHERE
        st_covers(st_buffer(other.geom, 0.0005), segment.geom)
        AND st_covers(st_buffer(segment.geom, 0.0005), other.geom)
),
-- CSCL-SAF-03 (gdb SegCount): prod counts AltSegmentData rows with a blank alt_segdata_type
-- in the segment's SegCount, as it writes them to the SAF; ours leaves them out (spec Table 13).
blank_type_altsegmentdata AS (
    SELECT
        lpad(segmentid::text, 7, '0') AS segmentid,
        count(*) AS n_rows
    FROM {{ source('recipe_sources', 'dcp_cscl_altsegmentdata') }}
    WHERE nullif(trim(alt_segdata_type), '') IS NULL
    GROUP BY 1
),
-- CSCL-SAF-06 (gdb SegCount): CommonPlaces both sides' SAF has but prod's LION gdb leaves
-- out. Ours writes their gdb N rows and counts them in the segment's SegCount.
vintage_commonplaces AS (
    SELECT
        lpad(segmentid::text, 7, '0') AS segmentid,
        count(*) AS n_rows
    FROM {{ source('recipe_sources', 'dcp_cscl_commonplace_gdb') }}
    WHERE placeid IN (1042178)
    GROUP BY 1
),
with_context AS (
    SELECT
        d.*,
        b._feature_type,
        b._source_table,
        b.featuretyp,
        split_part(d._gdb_lion_key, '|', 1) AS segmentid,
        tlr.segmentid IS NOT NULL AS is_topology_lr_segment,
        ncc.segmentid IS NOT NULL AS has_near_coincident_centerline,
        coalesce(bta.n_rows, 0) AS n_blank_type_altsegmentdata,
        coalesce(vcp.n_rows, 0) AS n_vintage_commonplaces
    FROM base_diffs AS d
    LEFT JOIN {{ ref('qa_int__build_gdb_lion_keyed') }} AS b ON d._gdb_lion_key = b._gdb_lion_key
    LEFT JOIN topology_lr_segments AS tlr ON split_part(d._gdb_lion_key, '|', 1) = tlr.segmentid
    LEFT JOIN near_coincident_centerlines AS ncc ON split_part(d._gdb_lion_key, '|', 1) = ncc.segmentid
    LEFT JOIN blank_type_altsegmentdata AS bta ON split_part(d._gdb_lion_key, '|', 1) = bta.segmentid
    LEFT JOIN vintage_commonplaces AS vcp ON split_part(d._gdb_lion_key, '|', 1) = vcp.segmentid
),
-- one row per changed field: cause label + whether the cause is verified/accepted
field_causes AS (
    SELECT
        w._gdb_lion_key,
        f.key,
        CASE
            WHEN
                f.key IN (
                    'llo_hyphen', 'lhi_hyphen', 'rlo_hyphen', 'rhi_hyphen',
                    'fromleft', 'toleft', 'fromright', 'toright'
                )
                THEN 'CSCL-LION-15: house numbers'
            -- prod has no zip for the Randall's Island proto-segments; ours is 10035: a
            -- difference only with fix_nonstreet_proto_zip on (off, the default, writes blank)
            WHEN
                {{ 'TRUE' if var('fix_nonstreet_proto_zip') else 'FALSE' }}
                AND f.key IN ('lzip', 'rzip')
                AND (w.changes -> f.key ->> 'old') IS NULL
                AND (w.changes -> f.key ->> 'new') = '10035'
                THEN 'CSCL-LION-02: Randall''s Island zip'
            WHEN f.key IN ('lzip', 'rzip') THEN 'Zip'
            WHEN
                f.key = 'segcount'
                AND w.has_near_coincident_centerline
                AND abs(
                    try_cast(w.changes ->> '$.segcount.old' AS int)
                    - try_cast(w.changes ->> '$.segcount.new' AS int)
                ) = 1
                THEN 'CSCL-LION-14: SegCount near-coincident centerline'
            WHEN
                f.key = 'segcount'
                AND w.n_blank_type_altsegmentdata > 0
                AND try_cast(w.changes ->> '$.segcount.old' AS int)
                - try_cast(w.changes ->> '$.segcount.new' AS int) = w.n_blank_type_altsegmentdata
                THEN 'CSCL-SAF-03: SegCount counts blank alt_segdata_type row'
            WHEN
                f.key = 'segcount'
                AND w.n_vintage_commonplaces > 0
                AND try_cast(w.changes ->> '$.segcount.new' AS int)
                - try_cast(w.changes ->> '$.segcount.old' AS int) = w.n_vintage_commonplaces
                THEN 'CSCL-SAF-06: SegCount counts CommonPlace prod lacks'
            -- not attributed to a known difference
            WHEN f.key = 'segcount' THEN 'SegCount'
            WHEN f.key = 'street' THEN 'Street'
            WHEN f.key = 'seqnum' AND w._feature_type = 'nonstreetfeatures'
                THEN 'CSCL-LION-05: nonstreet SeqNum'
            WHEN f.key = 'seqnum' THEN 'SeqNum on non-nonstreet feature'
            -- prod's pointer 1 when the BOE-preferred LGC value is above 9: a difference
            -- only with fix_boe_lgc_pointer on (off, the default, writes prod's pointer)
            WHEN
                {{ 'TRUE' if var('fix_boe_lgc_pointer') else 'FALSE' }}
                AND f.key = 'boe_lgc'
                AND (w.changes -> 'boe_lgc' ->> 'old') = '1'
                THEN 'CSCL-LION-04: BOE_LGC'
            WHEN f.key = 'boe_lgc' THEN 'BOE_LGC other'
            -- prod leaves a single-arc non-centerline segment blank: a difference only with
            -- fix_single_arc_curve_flag on (off, the default, writes prod's blank)
            WHEN
                {{ 'TRUE' if var('fix_single_arc_curve_flag') else 'FALSE' }}
                AND f.key = 'curveflag'
                AND coalesce(trim((w.changes -> 'curveflag' ->> 'old')), '') = ''
                AND (w.changes -> 'curveflag' ->> 'new') = 'I'
                THEN 'CSCL-LION-03: CurveFlag I where prod blank'
            WHEN f.key = 'curveflag' THEN 'CurveFlag other'
            WHEN
                (
                    regexp_matches(f.key, '^[lr](ct|cb)(2020|2010|2000|1990)(suf)?$')
                    OR f.key IN (
                        'latomicpolygon', 'ratomicpolygon', 'l_cd', 'r_cd', 'lassmdist', 'rassmdist',
                        'lelectdist', 'relectdist', 'splitelect', 'lschldist', 'rschldist', 'splitschl',
                        'locstatus', 'borobndry'
                    )
                )
                AND w.is_topology_lr_segment
                THEN 'CSCL-LION-16: topology left/right AP'
            WHEN
                regexp_matches(f.key, '^[lr](ct|cb)(2020|2010|2000|1990)(suf)?$')
                OR f.key IN (
                    'latomicpolygon', 'ratomicpolygon', 'l_cd', 'r_cd', 'lassmdist', 'rassmdist',
                    'lelectdist', 'relectdist', 'splitelect', 'lschldist', 'rschldist', 'splitschl',
                    'locstatus', 'borobndry'
                )
                THEN 'L/R AP fields, non-topology segment'
            ELSE f.key
        END AS cause
    FROM with_context AS w
    CROSS JOIN LATERAL (SELECT unnest(json_keys(w.changes)) AS key) AS f
    WHERE w.status = 'modified'
),
-- a cause is explained when its label starts with one of these known-difference IDs
cause_summary AS (
    SELECT
        _gdb_lion_key,
        array_agg(DISTINCT cause) FILTER (
            WHERE split_part(cause, ':', 1) IN (
                'CSCL-LION-02', 'CSCL-LION-03', 'CSCL-LION-04', 'CSCL-LION-05', 'CSCL-LION-14',
                'CSCL-LION-16', 'CSCL-SAF-03', 'CSCL-SAF-06'
            )
        ) AS explained_causes,
        array_agg(DISTINCT cause) FILTER (
            WHERE split_part(cause, ':', 1) NOT IN (
                'CSCL-LION-02', 'CSCL-LION-03', 'CSCL-LION-04', 'CSCL-LION-05', 'CSCL-LION-14',
                'CSCL-LION-16', 'CSCL-SAF-03', 'CSCL-SAF-06'
            )
        ) AS open_causes
    FROM field_causes
    GROUP BY _gdb_lion_key
),
categorized AS (
    SELECT
        w._gdb_lion_key,
        w.status,
        w.changes,
        w.output_file_id,
        w.comparison_column,
        w.build_table_name,
        w.production_table_name,
        w._feature_type,
        w._source_table,
        c.explained_causes,
        c.open_causes
    FROM with_context AS w
    LEFT JOIN cause_summary AS c ON w._gdb_lion_key = c._gdb_lion_key
)
SELECT
    _gdb_lion_key AS comparison_id,
    status,
    changes,
    output_file_id,
    CASE
        WHEN status != 'modified' THEN ''
        WHEN open_causes IS NULL AND len(explained_causes) = 1 THEN explained_causes[1]
        WHEN open_causes IS NULL THEN 'Multiple accounted causes'
        WHEN len(open_causes) = 1 THEN open_causes[1]
        ELSE ''
    END AS diff_group,
    -- subgroup: the open causes when mixed with accounted ones, else the feature source
    CASE
        WHEN status != 'modified' THEN ''
        WHEN open_causes IS NULL AND len(explained_causes) > 1 THEN array_to_string(explained_causes, ' + ')
        WHEN len(open_causes) > 1 THEN array_to_string(open_causes, ' + ')
        ELSE coalesce(_source_table, '')
    END AS subgroup,
    comparison_column,
    build_table_name,
    production_table_name,
    -- every changed field has a known cause (CSCL-LION-02/03/04/05/14/16, CSCL-SAF-03/06)
    coalesce(status = 'modified' AND open_causes IS NULL, FALSE) AS accounted_for
FROM categorized
