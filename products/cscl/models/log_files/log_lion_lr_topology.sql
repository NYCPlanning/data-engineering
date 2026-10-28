-- Attachment to log.csv, exported as log_lion_lr_topology.csv: every LION record (LION .dat
-- or LION gdb) whose segment gets a different left or right AtomicPolygon from the AP
-- topology edges it runs along (int__segment_topology_sides) than from prod's 2 ft offset
-- probe (int__segment_offsets), CSCL-LION-16. Every field derived from the side AP (census,
-- districts, zip, locational status, ...) follows it. Which one the build writes is the var
-- fix_lion_lr_from_topology (default false: prod's probe). Logged every build either way,
-- as CSCL-LOG-OURS-08; log.csv carries the count (log__lion_lr_topology).
WITH lion AS (
    SELECT * FROM {{ ref('int__lion') }}
    WHERE include_in_geosupport_lion OR include_in_bytes_lion
),
disagree AS (
    SELECT * FROM {{ ref('int__segment_atomicpolygons') }}
    WHERE
        topo_left_atomicid IS DISTINCT FROM probe_left_atomicid
        OR topo_right_atomicid IS DISTINCT FROM probe_right_atomicid
)
SELECT
    'warning' AS log_level,
    'lion side atomic polygon method' AS error_category,
    'CSCL-LOG-OURS-08' AS log_message_id,
    log_messages.effect,
    lion.globalid,
    lion.source_table AS source_feature_layer,
    'segmentid' AS record_id_type,
    lion.segmentid::VARCHAR AS record_id,
    lion.boroughcode,
    lion.face_code,
    lion.five_digit_street_code,
    disagree.probe_left_atomicid,
    disagree.probe_right_atomicid,
    disagree.topo_left_atomicid,
    disagree.topo_right_atomicid,
    disagree.topo_method,
    lion.left_atomicid AS left_atomicid_written,
    lion.right_atomicid AS right_atomicid_written,
    '[CSCL-LOG-OURS-08] ' || {{ log_message(
        "The {} feature with a GlobalID = {} (SegmentID = {}) has left/right AtomicPolygon "
        "{}/{} from prod's 2 ft probe and {}/{} from the AtomicPolygon topology; this build "
        "writes {}/{} (fix_lion_lr_from_topology = "
        ~ ("true" if var("fix_lion_lr_from_topology") else "false") ~ ").",
        [
            "lion.source_table", "lion.globalid", "lion.segmentid",
            "disagree.probe_left_atomicid", "disagree.probe_right_atomicid",
            "disagree.topo_left_atomicid", "disagree.topo_right_atomicid",
            "lion.left_atomicid", "lion.right_atomicid",
        ]
    ) }} AS message
FROM lion
INNER JOIN disagree ON lion.globalid = disagree.globalid
LEFT JOIN {{ ref('log_messages') }} AS log_messages ON log_messages.id = 'CSCL-LOG-OURS-08'
ORDER BY lion.boroughcode, lion.segmentid, lion.globalid
