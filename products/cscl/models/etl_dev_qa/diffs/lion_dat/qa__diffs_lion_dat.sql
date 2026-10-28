{{
  config(
    materialized='table',
    tags=['on_demand', 'diffs', 'diffs_lion_dat']
  )
}}

-- Compressed diff view showing one row per _lion_key with changes in jsonb
-- Includes status: modified, only_in_legacy, only_in_build

WITH base_diffs AS (
  {{ generate_diff_summary(
      old_relation=ref('qa_int__prod_citywide_lion_dat'),
      new_relation=ref('lion_dat_by_field'),
      primary_key='_lion_key',
      exclude_columns=['_source_table', 'filler_l38', 'filler_l45', 'filler_l64', 'filler_l65', 'filler_l89', 'filler_l199'],
      build_table_name='lion_dat_by_field',
      production_table_name='qa_int__prod_citywide_lion_dat'
  ) }}
),
with_borough AS (
    SELECT
        d.*,
        COALESCE(b.boroughcode, p.boroughcode) AS boroughcode,
        b._source_table,
        b.feature_type_code
    FROM base_diffs AS d
    LEFT JOIN {{ ref('lion_dat_by_field') }} AS b ON d._lion_key = b._lion_key
    LEFT JOIN {{ ref('qa_int__prod_citywide_lion_dat') }} AS p ON d._lion_key = p._lion_key
),
-- CSCL-LION-16: segments whose left or right AP comes from the topology and differs from
-- the 2 ft probe's (prod's method). Empty when lion_lr_from_topology is off.
-- probe_in_other_borough: the probe's AP is in another borough than the topology's on
-- some side (the AtomicPolygon ID starts with the borough code).
topology_lr_segments AS (
    SELECT
        segmentid,
        BOOL_OR(
            LEFT(left_atomicid, 1) IS DISTINCT FROM LEFT(probe_left_atomicid, 1)
            OR LEFT(right_atomicid, 1) IS DISTINCT FROM LEFT(probe_right_atomicid, 1)
        ) AS probe_in_other_borough
    FROM {{ ref('int__segment_atomicpolygons') }}
    WHERE
        left_atomicid IS DISTINCT FROM probe_left_atomicid
        OR right_atomicid IS DISTINCT FROM probe_right_atomicid
    GROUP BY segmentid
),
-- CSCL-LION-14: segments with a subway/rail-centerline coincidence candidate, from either
-- side (centerline matched to a subway/rail, or a subway/rail matched to a centerline)
coincident_rail_segments AS (
    SELECT all_segments_id AS segmentid
    FROM {{ ref('int__centerline_coincident_subway_or_rail_matches') }}
    WHERE distance < 1
    UNION
    SELECT trains_segment_id AS segmentid
    FROM {{ ref('int__centerline_coincident_subway_or_rail_matches') }}
    WHERE distance < 1
    UNION
    SELECT segmentid
    FROM {{ ref('int__noncenterline_coincident_segments') }}
    WHERE coinc_segment_feature_type = 'centerline' AND distance < 1
),
-- CSCL-LION-22: centerline protos named only in FeatureName, which prod drops
-- (CSCL-LOG-LION-15); CSCL-LION-23: records with a value cut to fit its slot, which prod
-- writes over-length (CSCL-LOG-LION-21). Both keyed like lion_dat_by_field.
logged_lion_globalids AS (
    SELECT
        globalid,
        log_message_id
    FROM {{ ref('log_name_table_warnings') }}
    WHERE log_message_id = 'CSCL-LOG-LION-15'
    UNION ALL
    SELECT
        globalid,
        log_message_id
    FROM {{ ref('log__lion_record_length') }}
),
logged_lion_records AS (
    SELECT
        lion.boroughcode || lion.face_code || LPAD(lion.segmentid::TEXT, 7, '0') AS lion_key,
        BOOL_OR(logged.log_message_id = 'CSCL-LOG-LION-15') AS is_featurename_proto,
        BOOL_OR(logged.log_message_id = 'CSCL-LOG-LION-21') AS is_cut_to_fit
    FROM logged_lion_globalids AS logged
    INNER JOIN {{ ref('int__lion') }} AS lion ON logged.globalid = lion.globalid
    WHERE lion.include_in_geosupport_lion
    GROUP BY lion.boroughcode || lion.face_code || LPAD(lion.segmentid::TEXT, 7, '0')
),
categorized AS (
    SELECT
        _lion_key,
        status,
        changes,
        boroughcode,
        _source_table,
        CASE boroughcode
            WHEN '1' THEN 'lion_dat_manhattan'
            WHEN '2' THEN 'lion_dat_bronx'
            WHEN '3' THEN 'lion_dat_brooklyn'
            WHEN '4' THEN 'lion_dat_queens'
            WHEN '5' THEN 'lion_dat_statenisland'
            ELSE 'lion_dat_unknown'
        END AS output_file_id,
        -- Get the keys from the changes jsonb
        JSON_KEYS(changes) AS change_keys,
        comparison_column,
        build_table_name,
        production_table_name,
        COALESCE(
            with_borough.status = 'only_in_build' AND llr.is_featurename_proto, FALSE
        ) AS is_featurename_proto,
        COALESCE(llr.is_cut_to_fit, FALSE) AS is_cut_to_fit,
        -- CSCL-LION-01: a reversal swaps the nodes, their XY and the left/right fields
        COALESCE(
            _source_table = 'altsegmentdata'
            AND LIST_CONTAINS(ARRAY[
                '0016558', '0343093', '0343094', '0343095', '0343096'
            ], RIGHT(with_borough._lion_key, 7))
            AND LIST_BOOL_AND(LIST_TRANSFORM(
                JSON_KEYS(changes),
                key -> REGEXP_MATCHES(key, '^((from|to)_(nodeid|x|y)|(left|right)_.*)$')
            )),
            FALSE
        ) AS is_lion_01_change,
        -- CSCL-LION-05: records whose sequence number is numbered on the fly (spec 2.5.1):
        -- non-street features, and proto-segments whose FEATURE_TYPE makes them non-street
        -- features. Everything else copies its source's, so a change there is a real diff.
        COALESCE(
            _source_table = 'nonstreetfeatures'
            OR (_source_table = 'altsegmentdata' AND feature_type_code IN ('3', '4', '7', '8')),
            FALSE
        ) AS is_numbered_on_the_fly,
        -- CSCL-LION-16: every changed field is one the side AP determines (plus the
        -- already-accepted segment_seqnum / CSCL-LION-04 boe_lgc_pointer). Borough boundary
        -- and zip changes count only where the probe's AP is in another borough (prod then
        -- marks a borough boundary, takes that side's zip and blanks the other side);
        -- elsewhere those rows need a look.
        (
            tlr.segmentid IS NOT NULL
            AND COALESCE(LIST_BOOL_AND(LIST_TRANSFORM(
                JSON_KEYS(changes),
                key -> (
                    REGEXP_MATCHES(key, '^(left|right)_')
                    OR key IN ('segment_locational_status', 'segment_seqnum')
                    OR (key = 'boe_lgc_pointer' AND (changes -> 'boe_lgc_pointer' ->> 'old') = '1')
                    OR (key IN ('borough_boundary_indicator', 'l_zip', 'r_zip') AND tlr.probe_in_other_borough)
                )
            )), TRUE)
        ) AS is_topology_lr_change,
        -- CSCL-LION-14: coincident_seg_count is off by one on a segment with a subway/rail-
        -- centerline candidate, and nothing else changed but CSCL-LION-03's curve flag on
        -- subway/rail.
        (
            crs.segmentid IS NOT NULL
            AND LIST_CONTAINS(JSON_KEYS(changes), 'coincident_seg_count')
            AND ABS(
                TRY_CAST(TRIM(changes ->> '$.coincident_seg_count.old') AS INT)
                - TRY_CAST(TRIM(changes ->> '$.coincident_seg_count.new') AS INT)
            ) = 1
            AND LIST_BOOL_AND(LIST_TRANSFORM(
                JSON_KEYS(changes),
                key -> (
                    key = 'coincident_seg_count'
                    OR (
                        key = 'curve_flag'
                        AND (changes ->> '$.curve_flag.old') = ' '
                        AND (changes ->> '$.curve_flag.new') = 'I'
                        AND with_borough._source_table IN ('subway', 'rail')
                    )
                )
            ))
        ) AS is_coincident_rail_change
    FROM with_borough
    LEFT JOIN topology_lr_segments AS tlr ON RIGHT(with_borough._lion_key, 7)::INT = tlr.segmentid
    LEFT JOIN coincident_rail_segments AS crs ON RIGHT(with_borough._lion_key, 7)::INT = crs.segmentid
    LEFT JOIN logged_lion_records AS llr ON with_borough._lion_key = llr.lion_key
)
SELECT
    _lion_key AS comparison_id,
    status,
    changes,
    output_file_id,
    -- Categorize based on which fields changed
    CASE
        -- CSCL-LION-22: a centerline proto named only in FeatureName; prod drops it.
        -- See CSCL-LION-22
        WHEN is_featurename_proto
            THEN 'CSCL-LION-22: centerline proto named only in FeatureName'
        -- CSCL-LION-23: a value cut to fit its slot; prod writes the record over-length.
        -- See CSCL-LION-23
        WHEN is_cut_to_fit
            THEN 'CSCL-LION-23: value cut to fit 400-character record'
        -- CSCL-LION-04: prod returns pointer 1 whenever the BOE-preferred LGC value is
        -- above 9
        WHEN
            status = 'modified'
            AND ARRAY_LENGTH(change_keys, 1) = 1
            AND change_keys[1] = 'boe_lgc_pointer'
            AND (changes -> 'boe_lgc_pointer' ->> 'old') = '1'
            THEN 'CSCL-LION-04: boe_lgc_pointer BOE LGC above 9'
        -- CSCL-LION-02: Randall's Island NSF segments missing zip code in production.
        -- Fingerprinted on the value transition (both sides blank -> 10035) rather than
        -- segmentid, since segmentids can be renumbered/reused across LION editions.
        -- See CSCL-LION-02
        WHEN
            status = 'modified'
            AND LIST_HAS_ALL(change_keys, ARRAY['l_zip', 'r_zip'])
            AND (changes -> 'l_zip' ->> 'new') = '10035'
            AND TRIM(changes ->> '$.l_zip.old') = ''
            AND (changes -> 'r_zip' ->> 'new') = '10035'
            AND TRIM(changes ->> '$.r_zip.old') = ''
            THEN 'CSCL-LION-02: Randalls Island missing zip'
        -- CSCL-LION-01: doubly-reversed protosegments not reversed in production. Fingerprinted on
        -- segmentid (unlike CSCL-LION-02) since this list has been independently verified stable
        -- since 2026-03-02 (~6 months, unchanged across a file move) - see the bug doc for the
        -- source-data-derived query to re-verify/re-derive this list if it's ever in doubt.
        -- See CSCL-LION-01
        WHEN
            status = 'modified'
            AND is_lion_01_change
            THEN 'CSCL-LION-01: doubly-reversed protosegments'
        -- CSCL-LION-17: individually reviewed by JR (Aug 2026) against source data in a spreadsheet
        -- and confirmed our new value is correct. Fingerprinted on _lion_key since these are a
        -- one-off reviewed batch, not a general field-pattern rule. The batch's coincident_seg_count
        -- records are covered by the CSCL-LION-14 rule instead.
        -- See CSCL-LION-17
        WHEN
            status = 'modified'
            AND _lion_key = '236590241972'
            AND change_keys = ARRAY['special_address_flag']
            THEN 'CSCL-LION-17: JR-reviewed diffs (Aug 2026)'
        -- CSCL-LION-19: special_address_flag leaks from a centerline record onto its sibling
        -- ALT_SEGDATA_TYPE = R (on/off-ramp) protosegment in production, contradicting the
        -- legacy ETL's own documented rule that ramps never carry a SAF flag. Ours is correct.
        -- See CSCL-LION-19
        WHEN
            status = 'modified'
            AND _lion_key = '493960174704'
            AND change_keys = ARRAY['special_address_flag']
            THEN 'CSCL-LION-19: SAF flag leaks onto ramp protosegment'
        -- CSCL-LION-14: prod's Esri shape-equality test and our geometric match disagree on a
        -- near-identical subway/rail and centerline (prod also writes 0 where the spec floors
        -- the count at 1). See CSCL-LION-14
        WHEN status = 'modified' AND is_coincident_rail_change
            THEN 'CSCL-LION-14: subway/rail coincident_seg_count'
        -- CSCL-LION-16: left/right AP from the AP topology, where prod's 2 ft probe
        -- overshoots the adjacent AP. See CSCL-LION-16.
        WHEN status = 'modified' AND is_topology_lr_change
            THEN 'CSCL-LION-16: topology left/right AP'
        -- CSCL-LION-05: sequence numbers numbered on the fly (non-street features and
        -- non-street-feature protos) differ from prod's. See CSCL-LION-05
        WHEN
            status = 'modified'
            AND change_keys = ARRAY['segment_seqnum']
            AND is_numbered_on_the_fly
            THEN 'CSCL-LION-05: nonstreet segment_seqnum'
        -- CSCL-LION-03: prod leaves a single-arc shoreline/subway/rail segment blank (its
        -- source shape has two points); ours writes I. Same rule as accounted_for below.
        -- See CSCL-LION-03
        WHEN
            status = 'modified'
            AND change_keys = ARRAY['curve_flag']
            AND (changes -> 'curve_flag' ->> 'old') = ' '
            AND (changes -> 'curve_flag' ->> 'new') = 'I'
            AND _source_table IN ('shoreline', 'subway', 'rail')
            THEN 'CSCL-LION-03: curve flag I on single-arc curves'
        -- If only one field changed, use that as the group name
        WHEN status = 'modified' AND ARRAY_LENGTH(change_keys, 1) = 1
            THEN change_keys[1]
        ELSE ''
    END AS diff_group,
    '' AS subgroup,
    comparison_column,
    build_table_name,
    production_table_name,
    -- Mark as accounted for if it's a known bug/expected difference
    -- CSCL-LION-22 and CSCL-LION-23: see diff_group
    is_featurename_proto OR is_cut_to_fit OR COALESCE(
        status = 'modified' AND (
            -- CSCL-LION-05: see diff_group
            (change_keys = ARRAY['segment_seqnum'] AND is_numbered_on_the_fly)
            -- CSCL-LION-04: prod's pointer is 1 when the BOE-preferred LGC is above 9.
            -- See CSCL-LION-04
            OR (
                change_keys = ARRAY['boe_lgc_pointer']
                AND (changes -> 'boe_lgc_pointer' ->> 'old') = '1'
            )
            -- CSCL-LION-03: curve flag on single-arc shoreline/subway/rail. See CSCL-LION-03
            OR (
                change_keys = ARRAY['curve_flag']
                AND (changes -> 'curve_flag' ->> 'old') = ' '
                AND (changes -> 'curve_flag' ->> 'new') = 'I'
                AND _source_table IN ('shoreline', 'subway', 'rail')
            )
            -- CSCL-LION-02: Randall's Island NSF segments missing zip code in production.
            -- Fingerprinted on the value transition (both sides blank -> 10035) rather than
            -- segmentid, since segmentids can be renumbered/reused across LION editions.
            -- See CSCL-LION-02
            OR (
                LIST_HAS_ALL(change_keys, ARRAY['l_zip', 'r_zip'])
                AND (changes -> 'l_zip' ->> 'new') = '10035'
                AND TRIM(changes ->> '$.l_zip.old') = ''
                AND (changes -> 'r_zip' ->> 'new') = '10035'
                AND TRIM(changes ->> '$.r_zip.old') = ''
            )
            -- CSCL-LION-01: doubly-reversed protosegments not reversed in production.
            -- See CSCL-LION-01
            -- Only the fields a reversal changes.
            OR is_lion_01_change
            -- CSCL-LION-17: individually reviewed by JR (Aug 2026) and confirmed correct.
            -- See CSCL-LION-17
            OR (_lion_key = '236590241972' AND change_keys = ARRAY['special_address_flag'])
            -- CSCL-LION-19: special_address_flag leaks onto an ALT_SEGDATA_TYPE = R protosegment
            -- in production. See CSCL-LION-19
            OR (_lion_key = '493960174704' AND change_keys = ARRAY['special_address_flag'])
            -- CSCL-LION-14: subway/rail coincident_seg_count off by one. See CSCL-LION-14
            OR is_coincident_rail_change
            -- CSCL-LION-16: left/right AP from the AP topology instead of the 2 ft probe.
            -- See CSCL-LION-16.
            OR is_topology_lr_change
        ),
        FALSE
    ) AS accounted_for
FROM categorized
