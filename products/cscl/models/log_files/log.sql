-- this could be done with dbt_utils.get_relations_by_pattern
-- and dbt_utils.union_relations
-- if we either remove dash from db names
-- or if dbt fixes a bug

{{ dbt_utils.union_relations(relations=[
    ref("log__lion_centerline_boro_mismatch"),
    ref("log__lion_centerline_or_proto_seglocstatus_mismatch"),
    ref("log__lion_protosegment_orphans"),
    ref("log__lion_segment_lgc_count"),
    ref("log__lion_segments_ap_boro_mismatch"),
    ref("log__lion_segments_missing_nypd"),
    ref("log__lion_segments_missing_aps"),
    ref("log__lion_segments_missing_facecode"),
    ref("log__lion_segments_missing_nodes"),
]) }}
