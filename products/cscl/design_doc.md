# CSCL - Extraction of LION and Geosupport Files

[stub]

## LION

#### Error reports

Errors are logged for the following conditions
- If a preferred B7SC is not found in StreetName or in FeatureName. This is reflected/measured by segments missing face codes (`log__lion_segments_missing_facecode`)
- If a segment has an endpoint that is not joined to a Node (`log__lion_segments_missing_nodes`)
- If a segment does not join to an Atomic Polygon on either side (`log__lion_segments_missing_aps`)
- If a centerline or protosegment has a SEGLOCSTATUS in its source table that differs from its calculated Segment Locational Status (`log__lion_centerline_or_proto_seglocstatus_mismatch`)
- If a segment does not have a joined Atomic Polygon sharing the same borough code as the segment (`log__lion_segments_ap_boro_mismatch`)
- If a segment does not join to a NYPD BEAT polygon (`log__lion_segments_missing_nypd`)
- If a protosegment does not share a Segment ID with a geometry-modeled segment (`log__lion_protosegment_orphans`)
