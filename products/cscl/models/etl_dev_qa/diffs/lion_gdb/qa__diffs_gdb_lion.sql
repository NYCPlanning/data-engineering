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
-- the 2 ft probe's (prod's method).
topology_lr_segments AS (
    SELECT DISTINCT lpad(segmentid::text, 7, '0') AS segmentid
    FROM {{ ref('int__segment_atomicpolygons') }}
    WHERE
        left_atomicid IS DISTINCT FROM probe_left_atomicid
        OR right_atomicid IS DISTINCT FROM probe_right_atomicid
),
with_context AS (
    SELECT
        d.*,
        b._feature_type,
        b._source_table,
        b.featuretyp,
        split_part(d._gdb_lion_key, '|', 1) AS segmentid,
        tlr.segmentid IS NOT NULL AS is_topology_lr_segment
    FROM base_diffs AS d
    LEFT JOIN {{ ref('qa_int__build_gdb_lion_keyed') }} AS b ON d._gdb_lion_key = b._gdb_lion_key
    LEFT JOIN topology_lr_segments AS tlr ON split_part(d._gdb_lion_key, '|', 1) = tlr.segmentid
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
            WHEN f.key IN ('lzip', 'rzip') THEN 'Zip'
            WHEN f.key = 'segcount' THEN 'CSCL-LION-06?: SegCount'
            WHEN f.key = 'street' THEN 'Street'
            WHEN f.key = 'seqnum' AND w._feature_type = 'nonstreetfeatures'
                THEN 'CSCL-LION-05: nonstreet SeqNum'
            WHEN f.key = 'seqnum' THEN 'SeqNum on non-nonstreet feature'
            -- prod's legacy default pointer '1' for two-digit LGC codes
            WHEN f.key = 'boe_lgc' AND w.changes -> 'boe_lgc' ->> 'old' = '1'
                THEN 'CSCL-LION-04: BOE_LGC'
            WHEN f.key = 'boe_lgc' THEN 'BOE_LGC other'
            WHEN
                f.key = 'curveflag'
                AND coalesce(trim(w.changes -> 'curveflag' ->> 'old'), '') = ''
                AND w.changes -> 'curveflag' ->> 'new' = 'I'
                THEN 'CSCL-LION-03: CurveFlag I where prod blank'
            WHEN f.key = 'curveflag' THEN 'CurveFlag other'
            WHEN
                (
                    f.key ~ '^[lr](ct|cb)(2020|2010|2000|1990)(suf)?$'
                    OR f.key IN (
                        'latomicpolygon', 'ratomicpolygon', 'l_cd', 'r_cd', 'lassmdist', 'rassmdist',
                        'lelectdist', 'relectdist', 'splitelect', 'lschldist', 'rschldist', 'splitschl',
                        'locstatus', 'borobndry'
                    )
                )
                AND w.is_topology_lr_segment
                THEN 'CSCL-LION-16: topology left/right AP'
            WHEN
                f.key ~ '^[lr](ct|cb)(2020|2010|2000|1990)(suf)?$'
                OR f.key IN (
                    'latomicpolygon', 'ratomicpolygon', 'l_cd', 'r_cd', 'lassmdist', 'rassmdist',
                    'lelectdist', 'relectdist', 'splitelect', 'lschldist', 'rschldist', 'splitschl',
                    'locstatus', 'borobndry'
                )
                THEN 'L/R AP fields, non-topology segment'
            ELSE f.key
        END AS cause
    FROM with_context AS w
    CROSS JOIN LATERAL jsonb_object_keys(w.changes) AS f (key)
    WHERE w.status = 'modified'
),
cause_summary AS (
    SELECT
        _gdb_lion_key,
        array_agg(DISTINCT cause) FILTER (
            WHERE cause LIKE 'CSCL-LION-0_:%' OR cause LIKE 'CSCL-LION-16:%'
        ) AS explained_causes,
        array_agg(DISTINCT cause) FILTER (
            WHERE NOT (cause LIKE 'CSCL-LION-0_:%' OR cause LIKE 'CSCL-LION-16:%')
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
        WHEN open_causes IS NULL AND cardinality(explained_causes) = 1 THEN explained_causes[1]
        WHEN open_causes IS NULL THEN 'Multiple accounted causes'
        WHEN cardinality(open_causes) = 1 THEN open_causes[1]
        ELSE ''
    END AS diff_group,
    -- subgroup: the open causes when mixed with accounted ones, else the feature source
    CASE
        WHEN status != 'modified' THEN ''
        WHEN open_causes IS NULL AND cardinality(explained_causes) > 1 THEN array_to_string(explained_causes, ' + ')
        WHEN cardinality(open_causes) > 1 THEN array_to_string(open_causes, ' + ')
        ELSE coalesce(_source_table, '')
    END AS subgroup,
    comparison_column,
    build_table_name,
    production_table_name,
    -- every changed field has a verified, accepted cause (data_issues.md CSCL-LION-03/04/05/16)
    coalesce(status = 'modified' AND open_causes IS NULL, FALSE) AS accounted_for
FROM categorized
