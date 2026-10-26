-- this could be done with dbt_utils.get_relations_by_pattern
-- and dbt_utils.union_relations
-- if we either remove dash from db names
-- or if dbt fixes a bug

{{ dbt_utils.union_relations(relations=[
    ref("log__lion_centerline_boro_mismatch"),
    ref("log__lion_centerline_curve"),
    ref("log__lion_centerline_or_proto_seglocstatus_mismatch"),
    ref("log__lion_protosegment_orphans"),
    ref("log__lion_segment_lgc_count"),
    ref("log__lion_segments_ap_boro_mismatch"),
    ref("log__lion_segments_missing_nypd"),
    ref("log__lion_segments_missing_aps"),
    ref("log__lion_segments_missing_facecode"),
    ref("log__lion_segments_missing_nodes"),
    ref("log__lion_nonstreetfeature_linetype"),
    ref("log__thinlion_lookup_not_found"),
    ref("log__saf_invalid_b7sc"),
    ref("log__saf_name_missing"),
    ref("log__export_file_summary"),
]) }}
