-- CSCL-LOG-OURS-27, every build: how many segment records (one per CSCL feature or
-- proto-segment) get a different segment locational status (LION L41, LION gdb
-- LocStatus) by prod's rule ('X' where the left and right AtomicPolygons, in the same
-- borough, have different 2010 census tracts) than by the spec's (their 2020 tracts),
-- i.e. what the var fix_seg_loc_status_2020_tract changes (CSCL-LION-11), and whether it
-- is on. One row, also when the count is 0.
WITH changed AS (
    SELECT prod_segment_locational_status
    FROM {{ ref('int__segment_locational_status') }}
    WHERE prod_segment_locational_status IS DISTINCT FROM spec_segment_locational_status
)
SELECT
    'warning' AS log_level,
    'segment locational status tract' AS error_category,
    'CSCL-LOG-OURS-27' AS log_message_id,
    NULL::VARCHAR AS globalid,
    NULL::VARCHAR AS source_feature_layer,
    NULL::VARCHAR AS record_id_type,
    NULL::VARCHAR AS record_id,
    {% if var('fix_seg_loc_status_2020_tract') -%}
    {{ log_message(
        "{} segment records ({} X by the 2010 tracts only, {} by the 2020 tracts only) get a "
        "different segment locational status by prod's rule (X where the left and right "
        "AtomicPolygons' 2010 census tracts differ) than by the spec's (their 2020 tracts). The "
        "spec's 2020 tracts are ON in this build (fix_seg_loc_status_2020_tract = true): L41 "
        "and LocStatus compare the 2020 tracts (CSCL-LION-11)",
        [
            "count(*)",
            "count(*) FILTER (WHERE prod_segment_locational_status = 'X')",
            "count(*) FILTER (WHERE prod_segment_locational_status IS NULL)",
        ]
    ) }}
    {%- else -%}
    {{ log_message(
        "{} segment records ({} X by the 2010 tracts only, {} by the 2020 tracts only) get a "
        "different segment locational status by prod's rule (X where the left and right "
        "AtomicPolygons' 2010 census tracts differ) than by the spec's (their 2020 tracts). The "
        "spec's 2020 tracts are OFF in this build (fix_seg_loc_status_2020_tract = false): L41 "
        "and LocStatus compare the 2010 tracts, as prod (CSCL-LION-11)",
        [
            "count(*)",
            "count(*) FILTER (WHERE prod_segment_locational_status = 'X')",
            "count(*) FILTER (WHERE prod_segment_locational_status IS NULL)",
        ]
    ) }}
    {%- endif %} AS message,
    count(*) AS record_count,
    NULL::VARCHAR AS output_filename
FROM changed
