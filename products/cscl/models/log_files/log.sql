-- this could be done with dbt_utils.get_relations_by_pattern
-- and dbt_utils.union_relations
-- if we either remove dash from db names
-- or if dbt fixes a bug

-- Each log__* model emits its detection rows with a log_message_id and the message body;
-- the effect comes from the log_messages seed and the "[<id>] " prefix is added here.
-- record_id is text: union_relations would otherwise take the first model's type.
WITH log_rows AS (
    {{ dbt_utils.union_relations(relations=[
        ref("log__lion_centerline_boro_mismatch"),
        ref("log__lion_centerline_curve"),
        ref("log__single_arc_curve_flag"),
        ref("log__lion_centerline_or_proto_seglocstatus_mismatch"),
        ref("log__lion_protosegment_orphans"),
        ref("log__lion_segment_lgc_count"),
        ref("log__lion_segments_ap_boro_mismatch"),
        ref("log__lion_segments_missing_nypd"),
        ref("log__nonstreet_proto_zip"),
        ref("log__lion_segments_missing_aps"),
        ref("log__lion_segments_missing_facecode"),
        ref("log__lion_segments_missing_nodes"),
        ref("log__lion_nonstreetfeature_linetype"),
        ref("log__thinlion_lookup_not_found"),
        ref("log__saf_invalid_b7sc"),
        ref("log__saf_name_missing"),
        ref("log__saf_segment_not_found"),
        ref("log__saf_s_checks"),
        ref("log__saf_d_daps_b5sc"),
        ref("log__saf_commonplace_proto"),
        ref("log__special_sedat_name_missing"),
        ref("log__gdb_lion_name_missing"),
        ref("log__face_code_missing"),
        ref("log__lion_level_codes"),
        ref("log__rpl_missing_roadbed"),
        ref("log__street_name_blank_lookup_key"),
        ref("log__export_file_summary"),
        ref("log__rpl_nodes_and_level_codes"),
        ref("log__saf_sos_indicator"),
        ref("log__sedat_name_missing"),
        ref("log__district_lookup_missing"),
        ref("log__lion_record_length"),
        ref("log__gdb_lion_value_changed"),
        ref("log__name_table_warnings"),
        ref("log__boe_lgc_pointer"),
        ref("log__lion_lr_topology"),
    ], column_override={"record_id": "varchar"}) }}
)

SELECT
    log_rows._dbt_source_relation,
    log_rows.log_level,
    log_rows.error_category,
    log_rows.log_message_id,
    log_messages.effect,
    log_rows.globalid,
    log_rows.source_feature_layer,
    log_rows.record_id_type,
    log_rows.record_id,
    FORMAT('[{}] {}', log_rows.log_message_id, log_rows.message) AS message,
    log_rows.record_count,
    log_rows.output_filename
FROM log_rows
LEFT JOIN {{ ref('log_messages') }} AS log_messages ON log_rows.log_message_id = log_messages.id
