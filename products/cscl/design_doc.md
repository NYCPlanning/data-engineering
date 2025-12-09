# Structure of docs

This document is organized by output file. Each of these has the following sections
- description/summary
- export format
- transformation logic
- error reporting

# Notes

## Formatting
There are many formatting rules for the various text outputs

### Justification and Fill
Probably self explanatory with just one-or two of these rows, but these are the possible values

| Format | Meaning | Example unformatted value | Formatted Value |
|-|-|-|-|
| RJSF | Right-justified, space-filled | 23 | `"  23"` |
| RJZF | Right-justified, zero-filled | 23 | `0023"` |
| LJSF | Left-justified, space-filled | 23 | `"23  "` |
| LJSF | Left-justified, zero-filled | 23 | `"2300"` |

### "Blank if none"
Not relevant for "space-filled" fields, but for zero-filled ones, a None value sometimes could be represented as `"0000"` or `"    "`. If a field is ZF but "blank if none", it would be the latter. This isn't relevant for a SF field because the result is the same either way.

### Census Tract Suffixes
[stub]

# Outputs

## Geosupport LION

### Description/Summary

Every row is a: cscl segment

Often refered to as "LION flat files", "LION dat", "LION dat files". In some sense this is the core of this pipeline, and by far the most complex set of transformations. This consists of 5 specific files
- ManhattanLION.dat
- BronxLION.dat
- BrooklynLION.dat
- QueensLION.dat
- StatenIslandLION.dat
Which all stem from the same data/table but are obviously filtered by borough. There is significant overlap with "Bytes LION", which is a geodatabase

### Format
(copied from seeds/text_formatting/text_formatting__lion_dat.csv)

|field_number|field_name|field_label|field_length|start_index|end_index|justify_and_fill|blank_if_none|
|------------|----------|-----------|------------|-----------|---------|----------------|-------------|
|L1|boroughcode|Borough|1|1|1|RFSF|false|
|L2|face_code|Face Code|4|2|5|RJZF|false|
|L3|segment_seqnum|Sequence Number|5|6|10|RJZF|false|
|L4|segmentid|Segment ID|7|11|17|RJZF|false|
|L5|five_digit_street_code|5-Digit Street Code (5SC)|5|18|22|RJZF|false|
|L6|lgc1|LGC1|2|23|24|RJZF|false|
|L7|lgc2|LGC2|2|25|26|RJZF|true|
|L8|lgc3|LGC3|2|27|28|RJZF|true|
|L9|lgc4|LGC4|2|29|30|RJZF|true|
|L10|boe_lgc_pointer|Board of Elections LGC Pointer|1|31|31|RJSF|true|
|L11|from_sectionalmap|From-Sectional Map|2|32|33|RJZF|false|
|L12|from_nodeid|From-Node ID|7|34|40|RJZF|false|
|L13|from_x|From-X Coordinate|7|41|47|RJZF|false|
|L14|from_y|From-Y Coordinate|7|48|54|RJZF|false|
|L15|to_sectionalmap|To-Sectional Map|2|55|56|RJZF|false|
|L16|to_nodeid|To-Node ID|7|57|63|RJZF|false|
|L17|to_x|To-X Coordinate|7|64|70|RJZF|false|
|L18|to_y|To-Y Coordinate|7|71|77|RJZF|false|
|L19|left_2000_census_tract_basic|Left 2000 Census Tract Basic|4|78|81|RJSF|false|
|L19_1|left_2000_census_tract_suffix|Left 2000 Census Tract Suffix|2|82|83|RJZF|true|
|L20|left_dynamic_block|Left Dynamic Block|3|84|86|RJSF|false|
|L21|l_low_hn|Left Low House Number|7|87|93|RJSF|false|
|L22|l_high_hn|Left High House Number|7|94|100|RJSF|false|
|L23|lsubsect|Left Dept of Sanitation Subsection|2|101|102|RJZF|true|
|L24|l_zip|Left Zip Code|5|103|107|RJZF|true|
|L25|left_assembly_district|Left Assembly District|2|108|109|RJZF|true|
|L26|left_election_district|Left Election District|3|110|112|RJZF|true|
|L27|left_school_district|Left School District|2|113|114|RJZF|true|
|L28|right_2000_census_tract_basic|Right 2000 Census Tract Basic|4|115|118|RJSF|false|
|L28_1|right_2000_census_tract_suffix|Right 2000 Census Tract Suffix|2|119|120|RJZF|true|
|L29|right_dynamic_block|Right Dynamic Block|3|121|123|RJSF|false|
|L30|r_low_hn|Right Low House Number|7|124|130|RJSF|false|
|L31|r_high_hn|Right High House Number|7|131|137|RJSF|false|
|L32|rsubsect|Right Dept of Sanitation Subsection|2|138|139|RJZF|true|
|L33|r_zip|Right Zip Code|5|140|144|RJZF|true|
|L34|right_assembly_district|Right Assembly District|2|145|146|RJZF|true|
|L35|right_election_district|Right Election District|3|147|149|RJZF|true|
|L36|right_school_district|Right School District|2|150|151|RJZF|true|
|L37|split_election_district_flag|Split Election District Flag|1|152|152|RJSF|false|
|L38|filler_l38|Filler (formerly Split Community School District Flag)|1|153|153|RJSF|false|
|L39|sandist_ind|Sanitation District Boundary Indicator|1|154|154|RJSF|false|
|L40|traffic_direction|Traffic Direction|1|155|155|RJSF|false|
|L41|segment_locational_status|Segment Locational Status|1|156|156|RJSF|false|
|L42|feature_type_code|Feature Type Code|1|157|157|RJSF|false|
|L43|nonped|Non-Pedestrian Flag|1|158|158|RJSF|false|
|L44|continuous_parity_flag|Continuous Parity Indicator|1|159|159|RJSF|false|
|L45|filler_l45|Filler (formerly the Near BQ-Boundary Flag)|1|160|160|RJSF|false|
|L46|borough_boundary_indicator|Borough Boundary Indicator|1|161|161|RJSF|false|
|L47|twisted_parity_flag|Twisted Parity Flag|1|162|162|RJSF|false|
|L48|special_address_flag|Special Address Flag|1|163|163|RJSF|false|
|L49|curve_flag|Curve Flag|1|164|164|RJSF|false|
|L50|center_of_curvature_x|Center of Curvature X-Coordinate|7|165|171|RJZF|false|
|L51|center_of_curvature_y|Center of Curvature Y-Coordinate|7|172|178|RJZF|false|
|L52|segment_length_ft|Segment Length in Feet|5|179|183|RJZF|false|
|L53|from_level_code|From Level Code|1|184|184|RJSF|false|
|L54|to_level_code|To Level Code|1|185|185|RJSF|false|
|L55|trafdir_ver_flag|Traffic Direction Verification Flag|1|186|186|RJSF|false|
|L56|segment_type|Segment Type Code|1|187|187|RJSF|false|
|L57|coincident_seg_count|Coincident Segment Counter|1|188|188|RJSF|false|
|L58|incex_flag|Include/Exclude Flag|1|189|189|RJSF|false|
|L59|rw_type|Roadway Type|2|190|191|RJSF|false|
|L60|physicalid|PHYSICALID|7|192|198|RJZF|true|
|L61|genericid|GENERICID|7|199|205|RJZF|true|
|L62|nypdid|NYPDID|7|206|212|RJZF|true|
|L63|fdnyid|FDNYID|7|213|219|RJZF|true|
|L64|filler_l64|Filler (formerly Left BLOCKFACEID)|7|220|226|RJSF|false|
|L65|filler_l65|Filler (formerly Right BLOCKFACEID)|7|227|233|RJSF|false|
|L66|status|STATUS|1|234|234|RJSF|false|
|L67|streetwidth_min|STREETWIDTH_MIN|3|235|237|RJSF|false|
|L68|streetwidth_irr|STREETWIDTH_IRR|1|238|238|RJSF|false|
|L69_1|bike_lane_1|BIKELANE_1|1|239|239|RJSF|false|
|L70|fcc|FCC|2|240|241|RJSF|false|
|L71|right_of_way_type|Right of Way Type|1|242|242|RJSF|false|
|L72|left_2010_census_tract_basic|Left 2010 Census Tract Basic|4|243|246|RJSF|false|
|L72|left_2010_census_tract_suffix|Left 2010 Census Tract Suffix|2|247|248|RJZF|true|
|L73|right_2010_census_tract_basic|Right 2010 Census Tract Basic|4|249|252|RJSF|false|
|L73|right_2010_census_tract_suffix|Right 2010 Census Tract Suffix|2|253|254|RJZF|true|
|L74|lgc5|LGC5|2|255|256|RJZF|true|
|L75|lgc6|LGC6|2|257|258|RJZF|true|
|L76|lgc7|LGC7|2|259|260|RJZF|true|
|L77|lgc8|LGC8|2|261|262|RJZF|true|
|L78|lgc9|LGC9|2|263|264|RJZF|true|
|L79|legacy_segmentid|Legacy SEGMENTID|7|265|271|RJZF|true|
|L80|left_2000_census_block_basic|LEFT CENSUS BLOCK 2000 BASIC|4|272|275|RJSF|false|
|L81|left_2000_census_block_suffix|LEFT CENSUS BLOCK 2000 SUFFIX|1|276|276|RJSF|false|
|L82|right_2000_census_block_basic|RIGHT CENSUS BLOCK 2000 BASIC|4|277|280|RJSF|false|
|L83|right_2000_census_block_suffix|RIGHT CENSUS BLOCK 2000 SUFFIX|1|281|281|RJSF|false|
|L84|left_2010_census_block_basic|LEFT CENSUS BLOCK 2010 BASIC|4|282|285|RJSF|false|
|L85|left_2010_census_block_suffix|LEFT CENSUS BLOCK 2010 SUFFIX|1|286|286|RJSF|false|
|L86|right_2010_census_block_basic|RIGHT CENSUS BLOCK 2010 BASIC|4|287|290|RJSF|false|
|L87|right_2010_census_block_suffix|RIGHT CENSUS BLOCK 2010 SUFFIX|1|291|291|RJSF|false|
|L88|snow_priority|SNOW PRIORITY|1|292|292|RJSF|false|
|L69_2|bike_lane_2|BIKELANE_2|2|293|294|RJSF|false|
|L67_2|streetwidth_max|STREET WIDTH MAX|3|295|297|RJSF|false|
|L89|filler_l89|Filler L89|3|298|300|RJSF|false|
|L90|l_blockfaceid|Left BLOCKFACEID|10|301|310|RJZF|true|
|L91|r_blockfaceid|Right BLOCKFACEID|10|311|320|RJZF|true|
|L92|number_travel_lanes|NUMBER TRAVEL LANES|2|321|322|RJSF|false|
|L93|number_park_lanes|NUMBER PARK LANES|2|323|324|RJSF|false|
|L94|number_total_lanes|NUMBER TOTAL LANES|2|325|326|RJSF|false|
|L95|bike_traffic_direction|BIKE TRAFFIC DIR|2|327|328|RJSF|false|
|L96|posted_speed|POSTED SPEED|2|329|330|RJSF|false|
|L97|left_nypd_service_area|Left NYPD Service Area|1|331|331|RJSF|false|
|L98|right_nypd_service_area|Right NYPD Service Area|1|332|332|RJSF|false|
|L99|truck_route_type|Truck Route Type|1|333|333|RJSF|false|
|L100|left_2020_census_tract_basic|LEFT 2020 CENSUS TRACT Basic|4|334|337|RJSF|false|
|L100|left_2020_census_tract_suffix|LEFT 2020 CENSUS TRACT Suffix|2|338|339|RJZF|true|
|L101|right_2020_census_tract_basic|RIGHT 2020 CENSUS TRACT Basic|4|340|343|RJSF|false|
|L101|right_2020_census_tract_suffix|RIGHT 2020 CENSUS TRACT Suffix|2|344|345|RJZF|true|
|L102|left_2020_census_block_basic|LEFT CENSUS BLOCK 2020 BASIC|4|346|349|RJSF|false|
|L103|left_2020_census_block_suffix|LEFT CENSUS BLOCK 2020 SUFFIX|1|350|350|RJSF|false|
|L104|right_2020_census_block_basic|RIGHT CENSUS BLOCK 2020 BASIC|4|351|354|RJSF|false|
|L105|right_2020_census_block_suffix|RIGHT CENSUS BLOCK 2020 SUFFIX|1|355|355|RJSF|false|
|L199|filler_l199|Filler L199|45|356|400|RJSF|false|
