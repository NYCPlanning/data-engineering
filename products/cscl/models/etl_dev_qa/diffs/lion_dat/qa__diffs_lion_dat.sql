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
        b._source_table
    FROM base_diffs AS d
    LEFT JOIN {{ ref('lion_dat_by_field') }} AS b ON d._lion_key = b._lion_key
    LEFT JOIN {{ ref('qa_int__prod_citywide_lion_dat') }} AS p ON d._lion_key = p._lion_key
),
-- CSCL-LION-16: segments whose left or right AP comes from the topology and differs from
-- the 2 ft probe's (prod's method). Empty when lion_lr_from_topology is off.
topology_lr_segments AS (
    SELECT DISTINCT segmentid
    FROM {{ ref('int__segment_atomicpolygons') }}
    WHERE
        left_atomicid IS DISTINCT FROM probe_left_atomicid
        OR right_atomicid IS DISTINCT FROM probe_right_atomicid
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
        -- CSCL-LION-16: every changed field is one the side AP determines (plus the
        -- already-accepted segment_seqnum / CSCL-LION-04 boe_lgc_pointer). Borough boundary
        -- and zip changes are left out on purpose: those rows need a look.
        (
            tlr.segmentid IS NOT NULL
            AND COALESCE(LIST_BOOL_AND(LIST_TRANSFORM(
                JSON_KEYS(changes),
                key -> (
                    REGEXP_MATCHES(key, '^(left|right)_')
                    OR key IN ('segment_locational_status', 'segment_seqnum')
                    OR (key = 'boe_lgc_pointer' AND (changes -> 'boe_lgc_pointer' ->> 'old') = '1')
                )
            )), TRUE)
        ) AS is_topology_lr_change
    FROM with_borough
    LEFT JOIN topology_lr_segments AS tlr ON RIGHT(with_borough._lion_key, 7)::int = tlr.segmentid
)
SELECT
    _lion_key AS comparison_id,
    status,
    changes,
    output_file_id,
    -- Categorize based on which fields changed
    CASE
        -- CSCL-LION-04: BOE LGC pointer incorrect for two-digit LGC codes
        -- Old value was always "1" due to legacy bug
        WHEN
            status = 'modified'
            AND ARRAY_LENGTH(change_keys, 1) = 1
            AND change_keys[1] = 'boe_lgc_pointer'
            AND (changes -> 'boe_lgc_pointer' ->> 'old') = '1'
            THEN 'CSCL-LION-04: boe_lgc_pointer two-digit codes'
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
            AND LIST_CONTAINS(ARRAY[
                '0016558', '0343093', '0343094', '0343095', '0343096'
            ], RIGHT(_lion_key, 7))
            THEN 'CSCL-LION-01: doubly-reversed protosegments'
        -- CSCL-LION-17: individually reviewed by JR (Aug 2026) against source data in a spreadsheet
        -- and confirmed our new value is correct. Fingerprinted on _lion_key since these are a
        -- one-off reviewed batch, not a general field-pattern rule - e.g. this does NOT mark
        -- every coincident_seg_count diff accounted for, only these specific records. 11 still
        -- differ on 26c; 101650330641 stops differing with lion_lr_from_topology on.
        -- See CSCL-LION-17
        WHEN
            status = 'modified'
            AND LIST_CONTAINS(ARRAY[
                '101650330641', '116000297614', '125250032941', '130400327255',
                '236590241972', '333280149383', '333280149678', '335640029283',
                '335640188023', '416140101502', '104308103717', '104620245781'
            ], _lion_key)
            THEN 'CSCL-LION-17: JR-reviewed diffs (Aug 2026)'
        -- CSCL-LION-19: special_address_flag leaks from a centerline record onto its sibling
        -- ALT_SEGDATA_TYPE = R (on/off-ramp) protosegment in production, contradicting the
        -- legacy ETL's own documented rule that ramps never carry a SAF flag. Ours is correct.
        -- See CSCL-LION-19
        WHEN
            status = 'modified'
            AND _lion_key = '493960174704'
            THEN 'CSCL-LION-19: SAF flag leaks onto ramp protosegment'
        -- CSCL-LION-14: prod's real, byte-verified BronxLION.dat has coincident_seg_count = 0 for
        -- this segment, which the ETL spec explicitly forbids (its own documented floor for
        -- "no coincidences" is '1', never '0') - our 1 is spec-correct, prod's 0 is the bug.
        -- See CSCL-LION-14
        WHEN
            status = 'modified'
            AND _lion_key = '211480359093'
            THEN 'CSCL-LION-14: prod coincident_seg_count = 0'
        -- CSCL-LION-16: left/right AP from the AP topology, where prod's 2 ft probe
        -- overshoots the adjacent AP. See data_issues.md CSCL-LION-16.
        WHEN status = 'modified' AND is_topology_lr_change
            THEN 'CSCL-LION-16: topology left/right AP'
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
    COALESCE(
        status = 'modified' AND (
            -- Bug: segment_seqnum can differ (legacy issue)
            change_keys = ARRAY['segment_seqnum']
            -- CSCL-LION-04: BOE LGC pointer incorrect for two-digit LGC codes
            -- See CSCL-LION-04
            -- Only when old value was "1" (the legacy default)
            OR (
                change_keys = ARRAY['boe_lgc_pointer']
                AND (changes -> 'boe_lgc_pointer' ->> 'old') = '1'
            )
            -- Curve flag: CompoundCurves in non-centerline features
            -- Legacy ETL doesn't detect irregular curves in shoreline/subway/rail
            -- New ETL correctly identifies them as 'I'
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
            OR LIST_CONTAINS(ARRAY[
                '0016558', '0343093', '0343094', '0343095', '0343096'
            ], RIGHT(_lion_key, 7))
            -- CSCL-LION-17: individually reviewed by JR (Aug 2026) and confirmed correct.
            -- See CSCL-LION-17
            OR LIST_CONTAINS(ARRAY[
                '101650330641', '116000297614', '125250032941', '130400327255',
                '236590241972', '333280149383', '333280149678', '335640029283',
                '335640188023', '416140101502', '104308103717', '104620245781'
            ], _lion_key)
            -- CSCL-LION-19: special_address_flag leaks onto an ALT_SEGDATA_TYPE = R protosegment
            -- in production. See CSCL-LION-19
            OR _lion_key = '493960174704'
            -- CSCL-LION-14: prod's coincident_seg_count = 0 for this segment, which the spec
            -- forbids (documented floor is '1', never '0') - one specific record only.
            -- See CSCL-LION-14
            OR _lion_key = '211480359093'
            -- CSCL-LION-16: left/right AP from the AP topology instead of the 2 ft probe.
            -- See data_issues.md CSCL-LION-16.
            OR is_topology_lr_change
        ),
        FALSE
    ) AS accounted_for
FROM categorized
