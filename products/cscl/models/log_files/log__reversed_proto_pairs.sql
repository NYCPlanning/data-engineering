-- CSCL-LOG-OURS-12, every build: how many LION records of a centerline's proto-segments
-- come out the other way round in prod, because an odd number of reversed proto-segments
-- before them on the same segment reversed the shared shape (CSCL-LION-01, var
-- fix_reversed_proto_pairs), and whether the fix is on in this build. One row, also when
-- the count is 0.
WITH changed AS (
    SELECT
        lion.lionkey,
        lion.include_in_geosupport_lion
    FROM {{ ref('int__lion') }} AS lion
    INNER JOIN {{ ref('int__protosegments') }} AS protos
        ON lion.globalid = protos.globalid
    WHERE
        (lion.include_in_geosupport_lion OR lion.include_in_bytes_lion)
        AND lion.source_table = 'altsegmentdata'
        AND protos.geometry_reversed_prod IS DISTINCT FROM protos.geometry_reversed_fixed
)
SELECT
    'warning' AS log_level,
    'reversed proto legacy bug' AS error_category,
    'CSCL-LOG-OURS-12' AS log_message_id,
    NULL::VARCHAR AS globalid,
    'altsegmentdata' AS source_feature_layer,
    NULL::VARCHAR AS record_id_type,
    NULL::VARCHAR AS record_id,
    {% if var('fix_reversed_proto_pairs') -%}
    {{ log_message(
        "{} LION proto-segment records ({} in the LION .dat files) follow an odd number of reversed "
        "proto-segments of the same centerline. The reversed proto fix is ON in this build "
        "(fix_reversed_proto_pairs = true): each is oriented by its own FROM_TO_INDICATOR, where prod "
        "writes its nodes, XY and left/right fields the other way round (CSCL-LION-01)",
        ["COUNT(*)", "COUNT(*) FILTER (WHERE include_in_geosupport_lion)"]
    ) }}
    {%- else -%}
    {{ log_message(
        "{} LION proto-segment records ({} in the LION .dat files) follow an odd number of reversed "
        "proto-segments of the same centerline. The reversed proto fix is OFF in this build "
        "(fix_reversed_proto_pairs = false): their nodes, XY and left/right fields are the other way "
        "round from their FROM_TO_INDICATOR, as prod (CSCL-LION-01)",
        ["COUNT(*)", "COUNT(*) FILTER (WHERE include_in_geosupport_lion)"]
    ) }}
    {%- endif %} AS message,
    COUNT(*) AS record_count,
    NULL::VARCHAR AS output_filename
FROM changed
