{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['release_num']},
      {'columns': ['old_segment_id']},
      {'columns': ['new_segment_id']},
    ]
) }}

-- CSCL's edit journal, and the source of every LDF record type except 'N'. Despite the
-- name it is the "LDF table" of the CSCL Phase III design; the rename never happened.
SELECT
    ldf_num,
    version_num,
    change_datetime,
    record_type,
    action_code,
    -- Rows for the release being cut carry no release_num -- GR stamps it at publish
    -- time, after the GDB snapshot we ingest is taken. Select on NULL, not on a value.
    nullif(trim(release_num), '') AS release_num,
    old_segment_id,
    new_segment_id,
    old_from_nodeid,
    old_to_nodeid,
    new_from_nodeid,
    new_to_nodeid
FROM {{ source("recipe_sources", "dcp_cscl_centerlinehistory") }}
