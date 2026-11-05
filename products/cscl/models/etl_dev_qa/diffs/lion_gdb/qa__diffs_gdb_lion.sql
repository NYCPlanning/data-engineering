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
-- cause behind the row (or the single cause left unexplained). A one-sided row is
-- accounted for when one_sided_cause names a known difference for it.

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
-- CSCL-LION-11: segments whose LocStatus is the spec's (2020 tracts) and not prod's (2010
-- tracts). A difference only with fix_seg_loc_status_2020_tract on: off (the default)
-- this is empty, so a LocStatus diff is unaccounted.
seg_loc_tract_segments AS (
    SELECT DISTINCT lpad(segmentid::text, 7, '0') AS segmentid
    FROM {{ ref('int__segment_locational_status') }}
    WHERE
        {{ 'TRUE' if var('fix_seg_loc_status_2020_tract') else 'FALSE' }}
        AND prod_segment_locational_status IS DISTINCT FROM spec_segment_locational_status
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
-- in the segment's SegCount, as it writes them to the SAF. A difference only with
-- fix_blank_type_altsegmentdata on, which leaves them out (spec Table 13): off (the
-- default) this is empty, so a SegCount diff is unaccounted.
blank_type_altsegmentdata AS (
    SELECT
        lpad(segmentid::text, 7, '0') AS segmentid,
        count(*) AS n_rows
    FROM {{ source('recipe_sources', 'dcp_cscl_altsegmentdata') }}
    WHERE
        {{ 'TRUE' if var('fix_blank_type_altsegmentdata') else 'FALSE' }}
        AND nullif(trim(alt_segdata_type), '') IS NULL
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
-- CSCL-SAF-03 (gdb row): the blank-type AltSegmentData rows, which prod writes as a B
-- replicant and ours leaves out with fix_blank_type_altsegmentdata on (off, the default,
-- writes them too, so this is empty)
blank_type_altsegmentdata_rows AS (
    SELECT
        lpad(segmentid::text, 7, '0') AS segmentid,
        nullif(trim(l_low_hn), '') AS l_low_hn,
        nullif(trim(r_low_hn), '') AS r_low_hn
    FROM {{ source('recipe_sources', 'dcp_cscl_altsegmentdata') }}
    WHERE
        {{ 'TRUE' if var('fix_blank_type_altsegmentdata') else 'FALSE' }}
        AND nullif(trim(alt_segdata_type), '') IS NULL
),
-- CSCL-SAF-06 (gdb row): CommonPlace SAF records prod's SAF ABCEGNPX files carry, keyed
-- the way a G/N/X replicant's Join_ID starts (B5SC, LGC1) and ends (SAF type)
prod_saf_commonplaces AS (
    SELECT DISTINCT
        segmentid,
        saftype,
        b5sc,
        lgc1
    FROM (
        SELECT * FROM {{ source('production_outputs', 'saf_abcegnpx_generic') }}
        UNION ALL
        SELECT * FROM {{ source('production_outputs', 'saf_abcegnpx_roadbed') }}
    )
    WHERE saftype IN ('G', 'N', 'X')
),
-- CSCL-LION-25: S/V replicants of HYPHEN_TYPE R address points, with the high house
-- number of their range (HOUSE_NUMBER_RANGE), which prod's gdb leaves blank. A difference
-- only with fix_hyphen_r_high_house_number on: off (the default) this is empty, so a high
-- house number diff is unaccounted.
hyphen_r_replicants AS (
    SELECT
        lpad(lion.segmentid::text, 7, '0') AS segmentid,
        rtrim(replicant.join_id) AS join_id,
        list(DISTINCT coalesce(replicant.l_high_hn, replicant.r_high_hn)) AS high_hns
    FROM {{ ref('int__saf_replicant_fields') }} AS replicant
    INNER JOIN {{ ref('int__address_points') }} AS ap ON replicant.saf_globalid = ap.globalid
    INNER JOIN {{ ref('int__lion') }} AS lion ON replicant.lionkey = lion.lionkey
    WHERE
        {{ 'TRUE' if var('fix_hyphen_r_high_house_number') else 'FALSE' }}
        AND replicant.saf_source_table = 'addresspoints'
        AND ap.hyphen_type = 'R'
    GROUP BY lion.segmentid, replicant.join_id
),
-- CSCL-LION-26: segments with a reversed proto-segment. A difference only with
-- fix_reversed_proto_census_fields on: off (the default) this is empty, so a census or
-- community district diff there is unaccounted.
reversed_proto_segments AS (
    SELECT DISTINCT lpad(segmentid::text, 7, '0') AS segmentid
    FROM {{ ref('stg__altsegmentdata_proto') }}
    WHERE {{ 'TRUE' if var('fix_reversed_proto_census_fields') else 'FALSE' }} AND reversed
),
-- CSCL-LION-27: prod's own LION .dat values for the fields its gdb carries from an older
-- extract, one row per (segment, field, value)
prod_dat_values AS (
    SELECT DISTINCT
        segmentid,
        field.key,
        field.value
    FROM {{ source('production_outputs', 'citywide_lion_dat') }}
    CROSS JOIN LATERAL (
        SELECT
            unnest(['lsubsect', 'rsubsect', 'physicalid', 'genericid']) AS key,
            unnest([
                nullif(trim(lsubsect), ''),
                nullif(trim(rsubsect), ''),
                nullif(ltrim(trim(physicalid), '0'), ''),
                nullif(ltrim(trim(genericid), '0'), '')
            ]) AS value
    ) AS field
),
-- CSCL-LION-28 (gdb L/R_PD_Service_Area): prod's .dat L97/L98 per segment, which ours
-- matches; prod's gdb contradicts it on segments where an NYPD beat boundary moved in the
-- source before 26c (inv-cze.3.1).
prod_dat_pd_service_area AS (
    SELECT
        segmentid,
        coalesce(trim(any_value(left_nypd_service_area)), '') AS l_pd_service_area,
        coalesce(trim(any_value(right_nypd_service_area)), '') AS r_pd_service_area
    FROM {{ source('production_outputs', 'citywide_lion_dat') }}
    GROUP BY segmentid
    HAVING count(DISTINCT concat_ws('|', left_nypd_service_area, right_nypd_service_area)) = 1
),
with_context AS (
    SELECT
        d.*,
        b._feature_type,
        b._source_table,
        b.featuretyp,
        b.segmenttyp,
        split_part(d._gdb_lion_key, '|', 1) AS segmentid,
        tlr.segmentid IS NOT NULL AS is_topology_lr_segment,
        sls.segmentid IS NOT NULL AS is_seg_loc_tract_segment,
        ncc.segmentid IS NOT NULL AS has_near_coincident_centerline,
        coalesce(bta.n_rows, 0) AS n_blank_type_altsegmentdata,
        coalesce(vcp.n_rows, 0) AS n_vintage_commonplaces,
        rps.segmentid IS NOT NULL AS has_reversed_proto,
        hrr.high_hns AS hyphen_r_high_hns,
        -- CSCL-LION-01: the doubly-reversed protosegments (as in qa__diffs_lion_dat)
        split_part(d._gdb_lion_key, '|', 1) IN (
            '0016558', '0343093', '0343094', '0343095', '0343096'
        ) AS is_lion_01_segment,
        CASE
            -- CSCL-SAF-03: prod's B replicant of a blank-type AltSegmentData row
            WHEN
                d.status = 'only_in_legacy'
                AND EXISTS (
                    SELECT 1 FROM blank_type_altsegmentdata_rows AS bt
                    WHERE
                        bt.segmentid = p.segmentid
                        AND coalesce(bt.l_low_hn, '') = coalesce(p.llo_hyphen, '')
                        AND coalesce(bt.r_low_hn, '') = coalesce(p.rlo_hyphen, '')
                )
                AND right(rtrim(p.join_id), 1) = 'B'
                THEN 'CSCL-SAF-03: B replicant of blank-type AltSegmentData row'
            -- CSCL-SAF-06: a CommonPlace replicant prod's SAF has but its gdb doesn't
            WHEN
                d.status = 'only_in_build'
                AND length(rtrim(b.join_id)) = 15
                AND EXISTS (
                    SELECT 1 FROM prod_saf_commonplaces AS psc
                    WHERE
                        psc.segmentid = b.segmentid
                        AND psc.saftype = right(rtrim(b.join_id), 1)
                        AND psc.b5sc = left(b.join_id, 6)
                        AND psc.lgc1 = substr(b.join_id, 7, 2)
                )
                THEN 'CSCL-SAF-06: CommonPlace replicant prod''s gdb lacks'
        END AS one_sided_cause,
        pdsa.l_pd_service_area AS prod_dat_l_pd_service_area,
        pdsa.r_pd_service_area AS prod_dat_r_pd_service_area
    FROM base_diffs AS d
    LEFT JOIN {{ ref('qa_int__build_gdb_lion_keyed') }} AS b ON d._gdb_lion_key = b._gdb_lion_key
    LEFT JOIN {{ ref('qa_int__prod_gdb_lion_keyed') }} AS p ON d._gdb_lion_key = p._gdb_lion_key
    LEFT JOIN reversed_proto_segments AS rps ON split_part(d._gdb_lion_key, '|', 1) = rps.segmentid
    LEFT JOIN hyphen_r_replicants AS hrr
        ON
            split_part(d._gdb_lion_key, '|', 1) = hrr.segmentid
            AND split_part(d._gdb_lion_key, '|', 2) = hrr.join_id
    LEFT JOIN topology_lr_segments AS tlr ON split_part(d._gdb_lion_key, '|', 1) = tlr.segmentid
    LEFT JOIN seg_loc_tract_segments AS sls ON split_part(d._gdb_lion_key, '|', 1) = sls.segmentid
    LEFT JOIN near_coincident_centerlines AS ncc ON split_part(d._gdb_lion_key, '|', 1) = ncc.segmentid
    LEFT JOIN blank_type_altsegmentdata AS bta ON split_part(d._gdb_lion_key, '|', 1) = bta.segmentid
    LEFT JOIN vintage_commonplaces AS vcp ON split_part(d._gdb_lion_key, '|', 1) = vcp.segmentid
    LEFT JOIN prod_dat_pd_service_area AS pdsa ON split_part(d._gdb_lion_key, '|', 1) = pdsa.segmentid
),
-- one row per changed field: cause label + whether the cause is verified/accepted
field_causes AS (
    SELECT
        w._gdb_lion_key,
        f.key,
        CASE
            -- prod keeps an F segment's house numbers: a difference only with
            -- zero_f_segment_house_numbers on (off, the default, keeps them too)
            WHEN
                {{ 'TRUE' if var('zero_f_segment_house_numbers') else 'FALSE' }}
                AND w.segmenttyp = 'F'
                AND f.key IN (
                    'llo_hyphen', 'lhi_hyphen', 'rlo_hyphen', 'rhi_hyphen',
                    'fromleft', 'toleft', 'fromright', 'toright'
                )
                THEN 'CSCL-LION-24: house numbers zeroed on SegmentTyp F'
            -- prod keeps a generic segment's house numbers: a difference only with
            -- zero_generic_house_numbers on (off, the default, keeps them too)
            WHEN
                {{ 'TRUE' if var('zero_generic_house_numbers') else 'FALSE' }}
                AND w.segmenttyp = 'G'
                AND f.key IN (
                    'llo_hyphen', 'lhi_hyphen', 'rlo_hyphen', 'rhi_hyphen',
                    'fromleft', 'toleft', 'fromright', 'toright'
                )
                AND coalesce(nullif(trim(w.changes -> f.key ->> 'new'), ''), '0') = '0'
                THEN 'CSCL-LION-15: house numbers zeroed on generic segments'
            -- prod keeps the house numbers of a FeatureTyp other than 0, 6 or W: a difference
            -- only with zero_feature_type_house_numbers on (off, the default, keeps them too)
            WHEN
                {{ 'TRUE' if var('zero_feature_type_house_numbers') else 'FALSE' }}
                AND w.featuretyp NOT IN ('0', '6', 'W')
                AND f.key IN (
                    'llo_hyphen', 'lhi_hyphen', 'rlo_hyphen', 'rhi_hyphen',
                    'fromleft', 'toleft', 'fromright', 'toright'
                )
                AND coalesce(nullif(trim(w.changes -> f.key ->> 'new'), ''), '0') = '0'
                THEN 'CSCL-LION-29: house numbers zeroed on FeatureTyp other than 0, 6 or W'
            -- CSCL-LION-25: prod writes no high house number on an S/V replicant of a
            -- HYPHEN_TYPE R address point; ours writes the range's HOUSE_NUMBER_RANGE with
            -- fix_hyphen_r_high_house_number on (hyphen_r_high_hns is empty with it off)
            WHEN
                f.key IN ('lhi_hyphen', 'rhi_hyphen', 'toleft', 'toright')
                AND coalesce(w.changes -> f.key ->> 'old', '0') = '0'
                AND list_contains(w.hyphen_r_high_hns, w.changes -> f.key ->> 'new')
                THEN 'CSCL-LION-25: high house number of a HYPHEN_TYPE R address point'
            -- CSCL-LION-26: on a reversed proto-segment prod swaps the AtomicPolygons but
            -- not the census and community district fields; ours swaps both with
            -- fix_reversed_proto_census_fields on, so each value is the other side's
            -- (has_reversed_proto is false with it off)
            WHEN
                (
                    regexp_matches(f.key, '^[lr](ct|cb)(2020|2010|2000|1990)(suf)?$')
                    OR f.key IN ('l_cd', 'r_cd')
                )
                AND w.has_reversed_proto
                AND list_contains(json_keys(w.changes), f.lr_partner)
                AND coalesce(w.changes -> f.key ->> 'old', '') = coalesce(w.changes -> f.lr_partner ->> 'new', '')
                AND coalesce(w.changes -> f.key ->> 'new', '') = coalesce(w.changes -> f.lr_partner ->> 'old', '')
                THEN 'CSCL-LION-26: reversed proto-segment census and CD fields'
            -- CSCL-LION-01: prod doesn't reverse the doubly-reversed protosegments: a
            -- difference only with fix_reversed_proto_pairs on (off, the default, writes
            -- prod's orientation)
            WHEN
                {{ 'TRUE' if var('fix_reversed_proto_pairs') else 'FALSE' }}
                AND w.is_lion_01_segment
                AND w._source_table = 'altsegmentdata'
                AND regexp_matches(f.key, '^(nodeid(from|to)|[xy](from|to)|[lr]atomicpolygon)$')
                THEN 'CSCL-LION-01: doubly-reversed protosegments'
            -- CSCL-LION-27: prod's gdb has an older value than its own .dat, which ours matches
            WHEN
                f.key IN ('lsubsect', 'rsubsect', 'physicalid', 'genericid')
                AND EXISTS (
                    SELECT 1 FROM prod_dat_values AS dat
                    WHERE
                        dat.segmentid = w.segmentid
                        AND dat.key = f.key
                        AND dat.value = f.dat_new
                )
                AND NOT EXISTS (
                    SELECT 1 FROM prod_dat_values AS dat
                    WHERE
                        dat.segmentid = w.segmentid
                        AND dat.key = f.key
                        AND coalesce(dat.value, '') = coalesce(f.dat_old, '')
                )
                THEN 'CSCL-LION-27: prod gdb older than its .dat'
            WHEN
                f.key IN (
                    'llo_hyphen', 'lhi_hyphen', 'rlo_hyphen', 'rhi_hyphen',
                    'fromleft', 'toleft', 'fromright', 'toright'
                )
                THEN 'House numbers'
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
            -- prod's LION gdb numbers non-street features in one extract session, its .dat
            -- files in the sessions of var lion_seqnum_extract_sessions: a difference only
            -- with the gdb numbered like the .dat files (lion_gdb_seqnum_extract_sessions set
            -- to them; the default numbers it like prod's gdb)
            WHEN
                {{ 'TRUE' if var('lion_gdb_seqnum_extract_sessions') == var('lion_seqnum_extract_sessions') else 'FALSE' }}
                AND f.key = 'seqnum'
                AND w._feature_type = 'nonstreetfeatures'
                THEN 'CSCL-LION-05: nonstreet SeqNum'
            WHEN f.key = 'seqnum' AND w._feature_type = 'nonstreetfeatures' THEN 'Nonstreet SeqNum'
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
            -- ours equals prod's .dat L97/L98 and prod's gdb differs from both
            WHEN
                f.key IN ('l_pd_service_area', 'r_pd_service_area')
                AND coalesce(trim(w.changes -> f.key ->> 'new'), '')
                != coalesce(trim(w.changes -> f.key ->> 'old'), '')
                AND coalesce(trim(w.changes -> f.key ->> 'new'), '') = CASE f.key
                    WHEN 'l_pd_service_area' THEN w.prod_dat_l_pd_service_area
                    ELSE w.prod_dat_r_pd_service_area
                END
                THEN 'CSCL-LION-28: PD service area, prod gdb contradicts prod .dat'
            -- CSCL-LION-11: LocStatus X by the 2020 census tracts, not prod's 2010, with
            -- fix_seg_loc_status_2020_tract on (is_seg_loc_tract_segment is false with it off)
            WHEN f.key = 'locstatus' AND w.is_seg_loc_tract_segment
                THEN 'CSCL-LION-11: LocStatus by 2020 tracts'
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
    CROSS JOIN
        LATERAL (
            SELECT
                keys.key,
                -- the other side's field of a left/right pair: lcb2010 <-> rcb2010, l_cd <-> r_cd
                (CASE WHEN left(keys.key, 1) = 'l' THEN 'r' ELSE 'l' END) || substr(keys.key, 2) AS lr_partner,
                -- the changed values as prod's .dat carries them: IDs without zero padding
                CASE
                    WHEN keys.key IN ('physicalid', 'genericid')
                        THEN ltrim(w.changes -> keys.key ->> 'old', '0')
                    ELSE w.changes -> keys.key ->> 'old'
                END AS dat_old,
                CASE
                    WHEN keys.key IN ('physicalid', 'genericid')
                        THEN ltrim(w.changes -> keys.key ->> 'new', '0')
                    ELSE w.changes -> keys.key ->> 'new'
                END AS dat_new
            FROM (SELECT unnest(json_keys(w.changes)) AS key) AS keys
        ) AS f
    WHERE w.status = 'modified'
),
-- a cause is explained when its label starts with one of these known-difference IDs
cause_summary AS (
    SELECT
        _gdb_lion_key,
        array_agg(DISTINCT cause) FILTER (
            WHERE split_part(cause, ':', 1) IN (
                'CSCL-LION-01', 'CSCL-LION-02', 'CSCL-LION-03', 'CSCL-LION-04', 'CSCL-LION-05',
                'CSCL-LION-11', 'CSCL-LION-14', 'CSCL-LION-15', 'CSCL-LION-16', 'CSCL-LION-24',
                'CSCL-LION-25', 'CSCL-LION-26', 'CSCL-LION-27', 'CSCL-LION-28', 'CSCL-LION-29',
                'CSCL-SAF-03', 'CSCL-SAF-06'
            )
        ) AS explained_causes,
        array_agg(DISTINCT cause) FILTER (
            WHERE split_part(cause, ':', 1) NOT IN (
                'CSCL-LION-01', 'CSCL-LION-02', 'CSCL-LION-03', 'CSCL-LION-04', 'CSCL-LION-05',
                'CSCL-LION-11', 'CSCL-LION-14', 'CSCL-LION-15', 'CSCL-LION-16', 'CSCL-LION-24',
                'CSCL-LION-25', 'CSCL-LION-26', 'CSCL-LION-27', 'CSCL-LION-28', 'CSCL-LION-29',
                'CSCL-SAF-03', 'CSCL-SAF-06'
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
        w.one_sided_cause,
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
        WHEN status != 'modified' THEN coalesce(one_sided_cause, '')
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
    -- modified: every changed field has a known cause; one-sided: one_sided_cause is set
    coalesce(
        CASE WHEN status = 'modified' THEN open_causes IS NULL ELSE one_sided_cause IS NOT NULL END,
        FALSE
    ) AS accounted_for
FROM categorized
