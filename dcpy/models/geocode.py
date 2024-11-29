from pydantic import BaseModel, Field


class Bbl(BaseModel):
    bbl: str = Field(alias="BOROUGH BLOCK LOT (BBL)")
    borough_code: str = Field(alias="Borough Code")
    tax_block: str = Field(alias="Tax Block")
    tax_lot: str = Field(alias="Tax Lot")


class UnitSortFormat(BaseModel):
    unit_sort_format: str = Field(alias="UNIT - SORT FORMAT")
    unit_type: str = Field(alias="Unit - Type")
    unit_identifier: str = Field(alias="Unit - Identifier")


class LowBblOfThisBuildingsCondominiumUnits(BaseModel):
    low_bbl_of_this_buildings_condominium_units: str = Field(
        alias="LOW BBL OF THIS BUILDING'S CONDOMINIUM UNITS"
    )
    borough_code: str = Field(alias="Borough Code")
    tax_block: str = Field(alias="Tax Block")
    tax_lot: str = Field(alias="Tax Lot")


class HighBblOfThisBuildingsCondominiumUnits(BaseModel):
    high_bbl_of_this_buildings_condominium_units: str = Field(
        alias="HIGH BBL OF THIS BUILDING'S CONDOMINIUM UNITS"
    )
    borough_code: str = Field(alias="Borough Code")
    tax_block: str = Field(alias="Tax Block")
    tax_lot: str = Field(alias="Tax Lot")


class SbvpSanbornMapIdentifier(BaseModel):
    sbvp: str = Field(alias="SBVP (SANBORN MAP IDENTIFIER)")
    sanborn_borough_code: str = Field(alias="Sanborn Borough Code")
    volume_number: str = Field(alias="Volume Number")
    volume_number_suffix: str = Field(alias="Volume Number Suffix")
    page_number: str = Field(alias="Page Number")
    page_number_suffix: str = Field(alias="Page Number Suffix")


class ListOfGeographicIdentifiers(BaseModel):
    low_house_number: str = Field(alias="Low House Number")
    high_house_number: str = Field(alias="High House Number")
    borough_code: str = Field(alias="Borough Code")
    five_digit_street_code: str = Field(alias="5-Digit Street Code")
    dcp_preferred_lgc: str = Field(alias="DCP-Preferred Local Group Code (LGC)")
    bin: str = Field(alias="Building Identification Number")
    side_of_street_indicator: str = Field(alias="Side of Street Indicator")
    geographic_identifier_entry_type_code: str = Field(
        alias="Geographic Identifier Entry Type Code"
    )
    tpad_bin_status: str = Field(alias="TPAD BIN Status")
    street_name: str | None = Field(alias="Street Name", default=None)


class LionKey(BaseModel):
    lion_key: str = Field(alias="LION KEY")
    borough_code: str = Field(alias="Borough Code")
    face_code: str = Field(alias="Face Code")
    sequence_number: str = Field(alias="Sequence Number")


class SpatialXYCoordinatesOfAddress(BaseModel):
    spatial_x_y_coordinates_of_address: str = Field(
        alias="SPATIAL X-Y COORDINATES OF ADDRESS"
    )
    x_coordinate: str = Field(alias="X Coordinate")
    y_coordinate: str = Field(alias="Y Coordinate")


class CommunityDistrict(BaseModel):
    community_district: str = Field(alias="COMMUNITY DISTRICT")
    community_district_borough_code: str = Field(
        alias="Community District Borough Code"
    )
    community_district_number: str = Field(alias="Community District Number")


class CityServiceInfo(BaseModel):
    health_center_district: str = Field(alias="Health Center District")
    health_area: str = Field(alias="Health Area")
    sanitation_district: str = Field(alias="Sanitation District")
    sanitation_collection_scheduling_section_and_subsection: str = Field(
        alias="Sanitation Collection Scheduling Section and Subsection"
    )
    sanitation_regular_collection_schedule: str = Field(
        alias="Sanitation Regular Collection Schedule"
    )
    sanitation_recycling_collection_schedule: str = Field(
        alias="Sanitation Recycling Collection Schedule"
    )
    police_patrol_borough_command: str = Field(alias="Police Patrol Borough Command")
    police_precinct: str = Field(alias="Police Precinct")
    fire_division: str = Field(alias="Fire Division")
    fire_battalion: str = Field(alias="Fire Battalion")
    fire_company_type: str = Field(alias="Fire Company Type")
    fire_company_number: str = Field(alias="Fire Company Number")
    community_school_district: str = Field(alias="Community School District")
    atomic_polygon: str = Field(alias="Atomic Polygon")
    police_patrol_borough: str = Field(alias="Police Patrol Borough")
    dot_street_light_contractor_area: str = Field(
        alias="DOT Street Light Contractor Area"
    )


class CensusInfo(BaseModel):
    borough_of_census_tract: str = Field(alias="Borough of Census Tract")
    census_tract_1990: str = Field(alias="1990 Census Tract")
    census_tract_2010: str = Field(alias="2010 Census Tract")
    census_block_2010: str = Field(alias="2010 Census Block")
    census_block_2010_suffix: str = Field(alias="2010 Census Block Suffix")
    census_tract_2000: str = Field(alias="2000 Census Tract")
    census_block_2000: str = Field(alias="2000 Census Block")
    census_block_2000_suffix: str = Field(alias="2000 Census Block Suffix")
    neighborhood_tabulation_area: str = Field(
        alias="Neighborhood Tabulation Area (NTA)"
    )
    dsny_snow_priority_code: str = Field(alias="DSNY Snow Priority Code")
    dsny_organic_recycling_schedule: str = Field(
        alias="DSNY Organic Recycling Schedule"
    )
    dsny_bulk_pickup_schedule: str = Field(alias="DSNY Bulk Pickup Schedule")
    hurricane_evacuation_zone: str = Field(alias="Hurricane Evacuation Zone (HEZ)")


class PoliticalInfo(BaseModel):
    election_district: str = Field(alias="Election District")
    assembly_district: str = Field(alias="Assembly District")
    split_election_district_flag: str = Field(alias="Split Election District Flag")
    congressional_district: str = Field(alias="Congressional District")
    state_senatorial_district: str = Field(alias="State Senatorial District")
    civil_court_district: str = Field(alias="Civil Court District")
    city_council_district: str = Field(alias="City Council District")


class BlockFace(BaseModel):  # 1, 1E
    pass


class GeoSupportReturn(BaseModel):
    reason_code: str = Field(alias="Reason Code")
    reason_code_qualifier: str = Field(alias="Reason Code Qualifier")
    warning_code: str = Field(alias="Warning Code")
    geosupport_return_code: str = Field(alias="Geosupport Return Code (GRC)")
    message: str = Field(alias="Message")
    reason_code_2: str = Field(alias="Reason Code 2")
    reason_code_qualifier_2: str = Field(alias="Reason Code Qualifier 2")
    warning_code_2: str = Field(alias="Warning Code 2")
    geosupport_return_code_2: str = Field(alias="Geosupport Return Code 2 (GRC 2)")


class Result1(CityServiceInfo, CensusInfo, PoliticalInfo, GeoSupportReturn):
    first_borough_name: str = Field(alias="First Borough Name")
    house_number_display_format: str = Field(alias="House Number - Display Format")
    house_number_sort_format: str = Field(alias="House Number - Sort Format")
    b10sc_first_borough_and_street_code: str = Field(
        alias="B10SC - First Borough and Street Code"
    )
    first_street_name_normalized: str = Field(alias="First Street Name Normalized")
    b10sc_second_borough_and_street_code: str = Field(
        alias="B10SC - Second Borough and Street Code"
    )
    second_street_name_normalized: str = Field(alias="Second Street Name Normalized")
    b10sc_third_borough_and_street_code: str = Field(
        alias="B10SC - Third Borough and Street Code"
    )
    third_street_name_normalized: str = Field(alias="Third Street Name Normalized")
    bbl: Bbl = Field(alias="BOROUGH BLOCK LOT (BBL)")
    filler_for_tax_lot_version_number: str = Field(
        alias="Filler for Tax Lot Version Number"
    )
    low_house_number_display_format: str = Field(
        alias="Low House Number - Display Format"
    )
    low_house_number_sort_format: str = Field(alias="Low House Number - Sort Format")
    bin: str = Field(alias="Building Identification Number (BIN)")
    street_attribute_indicators: str = Field(alias="Street Attribute Indicators")
    node_number: str = Field(alias="Node Number")
    unit_sort_format: UnitSortFormat = Field(alias="UNIT - SORT FORMAT")
    unit_display_format: str = Field(alias="Unit - Display Format")
    nin: str = Field(alias="NIN")
    street_attribute_indicator: str = Field(alias="Street Attribute Indicator")
    number_of_street_codes_and_street_names_in_list: str = Field(
        alias="Number of Street Codes and Street Names in List"
    )
    list_of_street_codes: list = Field(alias="List of Street Codes")
    list_of_street_names: list = Field(alias="List of Street Names")
    continuous_parity_duplicate_address_indicator: str = Field(
        alias="Continuous Parity Indicator/Duplicate Address Indicator"
    )
    low_house_number_of_block_face: str = Field(alias="Low House Number of Block Face")
    high_house_number_of_block_face: str = Field(
        alias="High House Number of Block Face"
    )
    dcp_preferred_lgc: str = Field(alias="DCP Preferred LGC")
    no_of_cross_streets_at_low_address_end: str = Field(
        alias="No. of Cross Streets at Low Address End"
    )
    list_of_cross_streets_at_low_address_end: str = Field(
        alias="List of Cross Streets at Low Address End"
    )
    no_of_cross_streets_at_high_address_end: str = Field(
        alias="No. of Cross Streets at High Address End"
    )
    list_of_cross_streets_at_high_address_end: str = Field(
        alias="List of Cross Streets at High Address End"
    )
    lion_key: LionKey = Field(alias="LION KEY")
    special_address_generated_record_flag: str = Field(
        alias="Special Address Generated Record Flag"
    )
    side_of_street_indicator: str = Field(alias="Side of Street Indicator")
    segment_length_in_feet: str = Field(alias="Segment Length in Feet")
    spatial_x_y_coordinates_of_address: SpatialXYCoordinatesOfAddress = Field(
        alias="SPATIAL X-Y COORDINATES OF ADDRESS"
    )
    reserved_for_possible_z_coordinate: str = Field(
        alias="Reserved for Possible Z Coordinate"
    )
    community_development_eligibility_indicator: str = Field(
        alias="Community Development Eligibility Indicator"
    )
    marble_hill_rikers_island_alternative_borough_flag: str = Field(
        alias="Marble Hill/Rikers Island Alternative Borough Flag"
    )
    community_district: CommunityDistrict = Field(alias="COMMUNITY DISTRICT")
    zip_code: str = Field(alias="ZIP Code")
    feature_type_code: str = Field(alias="Feature Type Code")
    segment_type_code: str = Field(alias="Segment Type Code")
    alley_or_cross_street_list_flag: str = Field(
        alias="Alley or Cross Street List Flag"
    )
    coincidence_segment_count: str = Field(alias="Coincidence Segment Count")
    underlying_address_number_on_true_street: str = Field(
        alias="Underlying Address Number on True Street"
    )
    underlying_b7sc_of_true_street: str = Field(alias="Underlying B7SC of True Street")
    segment_identifier: str = Field(alias="Segment Identifier")
    curve_flag: str = Field(alias="Curve Flag")


class Result1A(GeoSupportReturn):
    first_borough_name: str = Field(alias="First Borough Name")
    house_number_display_format: str = Field(alias="House Number - Display Format")
    house_number_sort_format: str = Field(alias="House Number - Sort Format")
    b10sc_first_borough_and_street_code: str = Field(
        alias="B10SC - First Borough and Street Code"
    )
    first_street_name_normalized: str = Field(alias="First Street Name Normalized")
    b10sc_second_borough_and_street_code: str = Field(
        alias="B10SC - Second Borough and Street Code"
    )
    second_street_name_normalized: str = Field(alias="Second Street Name Normalized")
    b10sc_third_borough_and_street_code: str = Field(
        alias="B10SC - Third Borough and Street Code"
    )
    third_street_name_normalized: str = Field(alias="Third Street Name Normalized")
    bbl: Bbl = Field(alias="BOROUGH BLOCK LOT (BBL)")
    filler_for_tax_lot_version_number: str = Field(
        alias="Filler for Tax Lot Version Number"
    )
    low_house_number_display_format: str = Field(
        alias="Low House Number - Display Format"
    )
    low_house_number_sort_format: str = Field(alias="Low House Number - Sort Format")
    bin: str = Field(alias="Building Identification Number (BIN)")
    street_attribute_indicators: str = Field(alias="Street Attribute Indicators")
    node_number: str = Field(alias="Node Number")
    unit_sort_format: UnitSortFormat = Field(alias="UNIT - SORT FORMAT")
    unit_display_format: str = Field(alias="Unit - Display Format")
    nin: str = Field(alias="NIN")
    street_attribute_indicator: str = Field(alias="Street Attribute Indicator")
    number_of_street_codes_and_street_names_in_list: str = Field(
        alias="Number of Street Codes and Street Names in List"
    )
    list_of_street_codes: list = Field(alias="List of Street Codes")
    list_of_street_names: list = Field(alias="List of Street Names")
    continuous_parity_duplicate_address_indicator: str = Field(
        alias="Continuous Parity Indicator /Duplicate Address Indicator"
    )
    low_house_number_of_defining_address_range: str = Field(
        alias="Low House Number of Defining Address Range"
    )
    rpad_self_check_code_for_bbl: str = Field(
        alias="RPAD Self-Check Code (SCC) for BBL"
    )
    rpad_building_classification_code: str = Field(
        alias="RPAD Building Classification Code"
    )
    corner_code: str = Field(alias="Corner Code")
    number_of_existing_structures_on_lot: str = Field(
        alias="Number of Existing Structures on Lot"
    )
    number_of_street_frontages_of_lot: str = Field(
        alias="Number of Street Frontages of Lot"
    )
    interior_lot_flag: str = Field(alias="Interior Lot Flag")
    vacant_lot_flag: str = Field(alias="Vacant Lot Flag")
    irregularly_shaped_lot_flag: str = Field(alias="Irregularly-Shaped Lot Flag")
    marble_hill_rikers_island_alternate_borough_flag: str = Field(
        alias="Marble Hill/Rikers Island Alternate Borough Flag"
    )
    lgi_overflow_flag: str = Field(
        alias="List of Geographic Identifiers (LGI) Overflow Flag"
    )
    strolling_key: str = Field(alias="STROLLING KEY")
    borough: str = Field(alias="Borough")
    five_digit_street_code_of_on_street: str = Field(
        alias="5-Digit Street Code of 'On' Street"
    )
    side_of_street_indicator: str = Field(alias="Side of Street Indicator")
    high_house_number_sort_format: str = Field(alias="High House Number - Sort Format")
    reserved_for_internal_use: str = Field(alias="Reserved for Internal Use")
    bin_of_input_address_or_nap: str = Field(
        alias="Building Identification Number (BIN) of Input Address or NAP"
    )
    condominium_flag: str = Field(alias="Condominium Flag")
    dof_condominium_identification_number: str = Field(
        alias="DOF Condominium Identification Number"
    )
    condominium_unit_id_number: str = Field(alias="Condominium Unit ID Number")
    condominium_billing_bbl: str = Field(alias="Condominium Billing BBL")
    filler_tax_lot_version_no_for_billing_bbl: str = Field(
        alias="Filler - Tax Lot Version No. for Billing BBL"
    )
    self_check_code_of_billing_bbl: str = Field(
        alias="Self-Check Code (SCC) of Billing BBL"
    )
    low_bbl_of_this_buildings_condominium_units: LowBblOfThisBuildingsCondominiumUnits = Field(
        alias="LOW BBL OF THIS BUILDING'S CONDOMINIUM UNITS"
    )
    filler_for_tax_lot_version_no_of_low_bbl: str = Field(
        alias="Filler for Tax Lot Version No. of Low BBL"
    )
    high_bbl_of_this_buildings_condominium_units: HighBblOfThisBuildingsCondominiumUnits = Field(
        alias="HIGH BBL OF THIS BUILDING'S CONDOMINIUM UNITS"
    )
    filler_for_tax_lot_version_no_of_high_bbl: str = Field(
        alias="Filler for Tax Lot Version No. of High BBL"
    )
    cooperative_id_number: str = Field(alias="Cooperative ID Number")
    sbvp_sanborn_map_identifier: SbvpSanbornMapIdentifier = Field(
        alias="SBVP (SANBORN MAP IDENTIFIER)"
    )
    dcp_commercial_study_area: str = Field(alias="DCP Commercial Study Area")
    tax_map_number_section_volume: str = Field(alias="Tax Map Number Section & Volume")
    reserved_for_tax_map_page_number: str = Field(
        alias="Reserved for Tax Map Page Number"
    )
    latitude: str = Field(alias="Latitude")
    longitude: str = Field(alias="Longitude")
    x_y_coordinates_of_tax_lot_centroid_internal_to_lot: str = Field(
        alias="X-Y Coordinates of Tax Lot Centroid (Internal to Lot)"
    )
    business_improvement_district: str = Field(
        alias="Business Improvement District (BID)"
    )
    tpad_bin_status_for_dm_job: str = Field(alias="TPAD BIN Status (for DM job)")
    tpad_new_bin: str = Field(alias="TPAD New BIN")
    tpad_new_bin_status: str = Field(alias="TPAD New BIN Status")
    tpad_conflict_flag: str = Field(alias="TPAD Conflict Flag")
    dcp_zoning_map: str = Field(alias="DCP Zoning Map")
    list_of_4_lgcs: str = Field(alias="List of 4 LGCs")
    number_of_entries_in_list_of_geographic_identifiers: str = Field(
        alias="Number of Entries in List of Geographic Identifiers"
    )
    list_of_geographic_identifiers: list[ListOfGeographicIdentifiers] = Field(
        alias="LIST OF GEOGRAPHIC IDENTIFIERS"
    )


class Result1E(CityServiceInfo, CensusInfo, PoliticalInfo, GeoSupportReturn):
    first_borough_name: str = Field(alias="First Borough Name")
    house_number_display_format: str = Field(alias="House Number - Display Format")
    house_number_sort_format: str = Field(alias="House Number - Sort Format")
    b10sc_first_borough_and_street_code: str = Field(
        alias="B10SC - First Borough and Street Code"
    )
    first_street_name_normalized: str = Field(alias="First Street Name Normalized")
    b10sc_second_borough_and_street_code: str = Field(
        alias="B10SC - Second Borough and Street Code"
    )
    second_street_name_normalized: str = Field(alias="Second Street Name Normalized")
    b10sc_third_borough_and_street_code: str = Field(
        alias="B10SC - Third Borough and Street Code"
    )
    third_street_name_normalized: str = Field(alias="Third Street Name Normalized")
    bbl: Bbl = Field(alias="BOROUGH BLOCK LOT (BBL)")
    filler_for_tax_lot_version_number: str = Field(
        alias="Filler for Tax Lot Version Number"
    )
    low_house_number_display_format: str = Field(
        alias="Low House Number - Display Format"
    )
    low_house_number_sort_format: str = Field(alias="Low House Number - Sort Format")
    building_identification_number_bin: str = Field(
        alias="Building Identification Number (BIN)"
    )
    street_attribute_indicators: str = Field(alias="Street Attribute Indicators")
    node_number: str = Field(alias="Node Number")
    unit_sort_format: UnitSortFormat = Field(alias="UNIT - SORT FORMAT")
    unit_display_format: str = Field(alias="Unit - Display Format")
    nin: str = Field(alias="NIN")
    street_attribute_indicator: str = Field(alias="Street Attribute Indicator")
    number_of_street_codes_and_street_names_in_list: str = Field(
        alias="Number of Street Codes and Street Names in List"
    )
    list_of_street_codes: list = Field(alias="List of Street Codes")
    list_of_street_names: list = Field(alias="List of Street Names")
    continuous_parity_indicator_duplicate_address_indicator: str = Field(
        alias="Continuous Parity Indicator/Duplicate Address Indicator"
    )
    low_house_number_of_block_face: str = Field(alias="Low House Number of Block Face")
    high_house_number_of_block_face: str = Field(
        alias="High House Number of Block Face"
    )
    dcp_preferred_lgc: str = Field(alias="DCP Preferred LGC")
    no_of_cross_streets_at_low_address_end: str = Field(
        alias="No. of Cross Streets at Low Address End"
    )
    list_of_cross_streets_at_low_address_end: str = Field(
        alias="List of Cross Streets at Low Address End"
    )
    no_of_cross_streets_at_high_address_end: str = Field(
        alias="No. of Cross Streets at High Address End"
    )
    list_of_cross_streets_at_high_address_end: str = Field(
        alias="List of Cross Streets at High Address End"
    )
    lion_key: LionKey = Field(alias="LION KEY")
    special_address_generated_record_flag: str = Field(
        alias="Special Address Generated Record Flag"
    )
    side_of_street_indicator: str = Field(alias="Side of Street Indicator")
    segment_length_in_feet: str = Field(alias="Segment Length in Feet")
    spatial_x_y_coordinates_of_address: SpatialXYCoordinatesOfAddress = Field(
        alias="SPATIAL X-Y COORDINATES OF ADDRESS"
    )
    reserved_for_possible_z_coordinate: str = Field(
        alias="Reserved for Possible Z Coordinate"
    )
    community_development_eligibility_indicator: str = Field(
        alias="Community Development Eligibility Indicator"
    )
    marble_hill_rikers_island_alternative_borough_flag: str = Field(
        alias="Marble Hill/Rikers Island Alternative Borough Flag"
    )
    community_district: CommunityDistrict = Field(alias="COMMUNITY DISTRICT")
    zip_code: str = Field(alias="ZIP Code")
    community_school_district: str = Field(alias="Community School District")
    atomic_polygon: str = Field(alias="Atomic Polygon")
    feature_type_code: str = Field(alias="Feature Type Code")
    segment_type_code: str = Field(alias="Segment Type Code")
    alley_or_cross_street_list_flag: str = Field(
        alias="Alley or Cross Street List Flag"
    )
    coincidence_segment_count: str = Field(alias="Coincidence Segment Count")
    underlying_address_number_on_true_street: str = Field(
        alias="Underlying Address Number on True Street"
    )
    underlying_b7_sc_of_true_street: str = Field(alias="Underlying B7SC of True Street")
    segment_identifier: str = Field(alias="Segment Identifier")
    curve_flag: str = Field(alias="Curve Flag")


class SpatialCoordinatesOfSegment(BaseModel):
    spatial_coordinates_of_segment: str = Field(alias="SPATIAL COORDINATES OF SEGMENT")
    x_coordinate_low_address_end: str = Field(alias="X Coordinate, Low Address End")
    y_coordinate_low_address_end: str = Field(alias="Y Coordinate, Low Address End")
    z_coordinate_low_address_end: str = Field(alias="Z Coordinate, Low Address End")
    x_coordinate_high_address_end: str = Field(alias="X Coordinate, High Address End")
    y_coordinate_high_address_end: str = Field(alias="Y Coordinate, High Address End")
    z_coordinate_high_address_end: str = Field(alias="Z Coordinate, High Address End")


class SpatialCoordinatesOfCenterOfCurvature(BaseModel):
    spatial_coordinates_of_center_of_curvature: str = Field(
        alias="SPATIAL COORDINATES OF CENTER OF CURVATURE"
    )
    x_coordinate: str = Field(alias="X Coordinate")
    y_coordinate: str = Field(alias="Y Coordinate")
    z_coordinate: str = Field(alias="Z Coordinate")


class SpatialCoordinatesOfActualSegment(BaseModel):
    spatial_coordinates_of_actual_segment: str = Field(
        alias="SPATIAL COORDINATES OF ACTUAL SEGMENT"
    )
    x_coordinate_low_address_end: str = Field(alias="X Coordinate, Low Address End")
    y_coordinate_low_address_end: str = Field(alias="Y Coordinate, Low Address End")
    z_coordinate_low_address_end: str = Field(alias="Z Coordinate, Low Address End")
    x_coordinate_high_address_end: str = Field(alias="X Coordinate, High Address End")
    y_coordinate_high_address_end: str = Field(alias="Y Coordinate, High Address End")
    z_coordinate_high_address_end: str = Field(alias="Z Coordinate, High Address End")


class StrollingKey(BaseModel):
    strolling_key: str = Field(alias="STROLLING KEY")
    borough: str = Field(alias="Borough")
    five_digit_street_code_of_on_street: str = Field(
        alias="5-Digit Street Code of ON- Street"
    )
    side_of_street_indicator: str = Field(alias="Side of Street Indicator")
    high_house_number: str = Field(alias="High House Number")


class Result1B(CensusInfo, GeoSupportReturn):
    first_borough_name: str = Field(alias="First Borough Name")
    house_number_display_format: str = Field(alias="House Number - Display Format")
    house_number_sort_format: str = Field(alias="House Number - Sort Format")
    b10_sc_first_borough_and_street_code: str = Field(
        alias="B10SC - First Borough and Street Code"
    )
    first_street_name_normalized: str = Field(alias="First Street Name Normalized")
    b10_sc_second_borough_and_street_code: str = Field(
        alias="B10SC - Second Borough and Street Code"
    )
    second_street_name_normalized: str = Field(alias="Second Street Name Normalized")
    b10_sc_third_borough_and_street_code: str = Field(
        alias="B10SC - Third Borough and Street Code"
    )
    third_street_name_normalized: str = Field(alias="Third Street Name Normalized")
    borough_block_lot_bbl: Bbl = Field(alias="BOROUGH BLOCK LOT (BBL)")
    filler_for_tax_lot_version_number: str = Field(
        alias="Filler for Tax Lot Version Number"
    )
    low_house_number_display_format: str = Field(
        alias="Low House Number - Display Format"
    )
    low_house_number_sort_format: str = Field(alias="Low House Number - Sort Format")
    bin: str = Field(alias="Building Identification Number (BIN)")
    street_attribute_indicators: str = Field(alias="Street Attribute Indicators")
    node_number: str = Field(alias="Node Number")
    unit_sort_format: UnitSortFormat = Field(alias="UNIT - SORT FORMAT")
    unit_display_format: str = Field(alias="Unit - Display Format")
    nin: str = Field(alias="NIN")
    street_attribute_indicator: str = Field(alias="Street Attribute Indicator")
    number_of_street_codes_and_street_names_in_list: str = Field(
        alias="Number of Street Codes and Street Names in List"
    )
    list_of_street_codes: list = Field(alias="List of Street Codes")
    list_of_street_names: list = Field(alias="List of Street Names")
    continuous_parity_indicator_duplicate_address_indicator: str = Field(
        alias="Continuous Parity Indicator/Duplicate Address Indicator"
    )
    low_house_number_of_block_face: str = Field(alias="Low House Number of Block Face")
    high_house_number_of_block_face: str = Field(
        alias="High House Number of Block Face"
    )
    dcp_preferred_lgc: str = Field(alias="DCP Preferred LGC")
    number_of_cross_streets_at_low_address_end: str = Field(
        alias="Number of Cross Streets at Low Address End"
    )
    list_of_cross_streets_at_low_address_end: str = Field(
        alias="List of Cross Streets at Low Address End"
    )
    number_of_cross_streets_at_high_address_end: str = Field(
        alias="Number of Cross Streets at High Address End"
    )
    list_of_cross_streets_at_high_address_end: str = Field(
        alias="List of Cross Streets at High Address End"
    )
    lion_key: LionKey = Field(alias="LION KEY")
    special_address_generated_record_flag: str = Field(
        alias="Special Address Generated Record Flag"
    )
    side_of_street_indicator: str = Field(alias="Side of Street Indicator")
    segment_length_in_feet: str = Field(alias="Segment Length in Feet")
    spatial_x_y_coordinates_of_address: str = Field(
        alias="Spatial X-Y Coordinates of Address"
    )
    reserved_for_possible_z_coordinate: str = Field(
        alias="Reserved for Possible Z Coordinate"
    )
    community_development_eligibility_indicator: str = Field(
        alias="Community Development Eligibility Indicator"
    )
    marble_hill_rikers_island_alternative_borough_flag: str = Field(
        alias="Marble Hill/Rikers Island Alternative Borough Flag"
    )
    dot_street_light_contractor_area: str = Field(
        alias="DOT Street Light Contractor Area"
    )
    community_district: CommunityDistrict = Field(alias="COMMUNITY DISTRICT")
    zip_code: str = Field(alias="ZIP Code")
    election_district: str = Field(alias="Election District")
    assembly_district: str = Field(alias="Assembly District")
    split_election_district_flag: str = Field(alias="Split Election District Flag")
    congressional_district: str = Field(alias="Congressional District")
    state_senatorial_district: str = Field(alias="State Senatorial District")
    civil_court_district: str = Field(alias="Civil Court District")
    city_council_district: str = Field(alias="City Council District")
    health_center_district: str = Field(alias="Health Center District")
    health_area: str = Field(alias="Health Area")
    sanitation_district: str = Field(alias="Sanitation District")
    sanitation_collection_scheduling_section_and_subsection: str = Field(
        alias="Sanitation Collection Scheduling Section and Subsection"
    )
    sanitation_regular_collection_schedule: str = Field(
        alias="Sanitation Regular Collection Schedule"
    )
    sanitation_recycling_collection_schedule: str = Field(
        alias="Sanitation Recycling Collection Schedule"
    )
    police_patrol_borough_command: str = Field(alias="Police Patrol Borough Command")
    police_precinct: str = Field(alias="Police Precinct")
    fire_division: str = Field(alias="Fire Division")
    fire_battalion: str = Field(alias="Fire Battalion")
    fire_company_type: str = Field(alias="Fire Company Type")
    fire_company_number: str = Field(alias="Fire Company Number")
    community_school_district: str = Field(alias="Community School District")
    atomic_polygon: str = Field(alias="Atomic Polygon")
    police_patrol_borough: str = Field(alias="Police Patrol Borough")
    feature_type_code: str = Field(alias="Feature Type Code")
    segment_type_code: str = Field(alias="Segment Type Code")
    alley_or_cross_street_list_flag: str = Field(
        alias="Alley or Cross Street List Flag"
    )
    coincidence_segment_count: str = Field(alias="Coincidence Segment Count")
    dsny_snow_priority_code: str = Field(alias="DSNY Snow Priority Code")
    dsny_organic_recycling_schedule: str = Field(
        alias="DSNY Organic Recycling Schedule"
    )
    dsny_bulk_pickup_schedule: str = Field(alias="DSNY Bulk Pickup Schedule")
    hurricane_evacuation_zone_hez: str = Field(alias="Hurricane Evacuation Zone (HEZ)")
    underlying_address_number_for_na_ps: str = Field(
        alias="Underlying Address Number for NAPs"
    )
    underlying_b7_sc: str = Field(alias="Underlying B7SC")
    segment_identifier: str = Field(alias="Segment Identifier")
    curve_flag: str = Field(alias="Curve Flag")
    list_of_4_lg_cs: str = Field(alias="List of 4 LGCs")
    boe_lgc_pointer: str = Field(alias="BOE LGC Pointer")
    segment_azimuth: str = Field(alias="Segment Azimuth")
    segment_orientation: str = Field(alias="Segment Orientation")
    spatial_coordinates_of_segment: SpatialCoordinatesOfSegment = Field(
        alias="SPATIAL COORDINATES OF SEGMENT"
    )
    spatial_coordinates_of_center_of_curvature: SpatialCoordinatesOfCenterOfCurvature = Field(
        alias="SPATIAL COORDINATES OF CENTER OF CURVATURE"
    )
    radius_of_circle: str = Field(alias="Radius of Circle")
    secant_location_related_to_curve: str = Field(
        alias="Secant Location Related to Curve"
    )
    angle_to_from_node_beta_value: str = Field(alias="Angle to From Node - Beta Value")
    angle_to_to_node_alpha_value: str = Field(alias="Angle to To Node - Alpha Value")
    from_lion_node_id: str = Field(alias="From LION Node ID")
    to_lion_node_id: str = Field(alias="To LION Node ID")
    lion_key_for_vanity_address: str = Field(alias="LION Key for Vanity Address")
    side_of_street_of_vanity_address: str = Field(
        alias="Side of Street of Vanity Address"
    )
    split_low_house_number: str = Field(alias="Split Low House Number")
    traffic_direction: str = Field(alias="Traffic Direction")
    turn_restrictions: str = Field(alias="Turn Restrictions")
    fraction_for_curve_calculation: str = Field(alias="Fraction for Curve Calculation")
    roadway_type: str = Field(alias="Roadway Type")
    physical_id: str = Field(alias="Physical ID")
    generic_id: str = Field(alias="Generic ID")
    nypd_id: str = Field(alias="NYPD ID")
    fdny_id: str = Field(alias="FDNY ID")
    bike_lane_2: str = Field(alias="Bike Lane 2")
    bike_traffic_direction: str = Field(alias="Bike Traffic Direction")
    street_status: str = Field(alias="Street Status")
    street_width: str = Field(alias="Street Width")
    street_width_irregular: str = Field(alias="Street Width Irregular")
    bike_lane: str = Field(alias="Bike Lane")
    federal_classification_code: str = Field(alias="Federal Classification Code")
    right_of_way_type: str = Field(alias="Right Of Way Type")
    list_of_second_set_of_5_lg_cs: str = Field(alias="List of Second Set of 5 LGCs")
    legacy_segment_id: str = Field(alias="Legacy Segment ID")
    from_preferred_lg_cs_first_set_of_5: str = Field(
        alias="From Preferred LGCs First Set of 5"
    )
    to_preferred_lg_cs_first_set_of_5: str = Field(
        alias="To Preferred LGCs First Set of 5"
    )
    from_preferred_lg_cs_second_set_of_5: str = Field(
        alias="From Preferred LGCs Second Set of 5"
    )
    to_preferred_lg_cs_second_set_of_5: str = Field(
        alias="To Preferred LGCs Second Set of 5"
    )
    no_cross_street_calculation_flag: str = Field(
        alias="No Cross Street Calculation Flag"
    )
    individual_segment_length: str = Field(alias="Individual Segment Length")
    nta_name: str = Field(alias="NTA Name")
    usps_preferred_city_name: str = Field(alias="USPS Preferred City Name")
    latitude: str = Field(alias="Latitude")
    longitude: str = Field(alias="Longitude")
    from_actual_segment_node_id: str = Field(alias="From Actual Segment Node ID")
    to_actual_segment_node_id: str = Field(alias="To Actual Segment Node ID")
    spatial_coordinates_of_actual_segment: SpatialCoordinatesOfActualSegment = Field(
        alias="SPATIAL COORDINATES OF ACTUAL SEGMENT"
    )
    blockface_id: str = Field(alias="Blockface ID")
    number_of_travel_lanes_on_the_street: str = Field(
        alias="Number of Travel Lanes on the Street"
    )
    number_of_parking_lanes_on_the_street: str = Field(
        alias="Number of Parking Lanes on the Street"
    )
    number_of_total_lanes_on_the_street: str = Field(
        alias="Number of Total Lanes on the Street"
    )
    street_width_maximum: str = Field(alias="Street Width Maximum")
    speed_limit: str = Field(alias="Speed Limit")
    puma_code: str = Field(alias="PUMA Code")
    police_sector: str = Field(alias="Police Sector")
    police_service_area: str = Field(alias="Police Service Area")
    truck_route_type: str = Field(alias="Truck Route Type")
    census_tract_2020: str = Field(alias="2020 Census Tract")
    census_block_2020: str = Field(alias="2020 Census Block")
    census_block_2020_suffix: str = Field(alias="2020 Census Block Suffix")
    neighborhood_tabulation_area_2020: str = Field(
        alias="2020 Neighborhood Tabulation Area (NTA)"
    )
    community_district_tabulation_area_2020: str = Field(
        alias="2020 Community District Tabulation Area (CDTA)"
    )
    filler: str = Field(alias="Filler")
    return_code: str = Field(alias="Return Code")
    no_of_cross_streets_at_high_address_end: str = Field(
        alias="No. of Cross Streets at High Address End"
    )
    list_of_cross_street_names_at_low_address_end: str = Field(
        alias="List of Cross Street Names at Low Address End"
    )
    list_of_cross_street_names_at_high_address_end: str = Field(
        alias="List of Cross Street Names at High Address End"
    )
    boe_preferred_b7_sc: str = Field(alias="BOE Preferred B7SC")
    boe_preferred_street_name: str = Field(alias="BOE Preferred Street Name")
    continuous_parity_indicator_duplicate_address_indicator: str = Field(
        alias="Continuous Parity Indicator / Duplicate Address Indicator"
    )
    low_house_number_of_defining_address_range: str = Field(
        alias="Low House Number of Defining Address Range"
    )
    rpad_self_check_code_scc_for_bbl: str = Field(
        alias="RPAD Self-Check Code (SCC) for BBL"
    )
    rpad_building_classification_code: str = Field(
        alias="RPAD Building Classification Code"
    )
    corner_code: str = Field(alias="Corner Code")
    number_of_existing_structures_on_lot: str = Field(
        alias="Number of Existing Structures on Lot"
    )
    number_of_street_frontages_of_lot: str = Field(
        alias="Number of Street Frontages of Lot"
    )
    interior_lot_flag: str = Field(alias="Interior Lot Flag")
    vacant_lot_flag: str = Field(alias="Vacant Lot Flag")
    irregularly_shaped_lot_flag: str = Field(alias="Irregularly-Shaped Lot Flag")
    marble_hill_rikers_island_alternate_borough_flag: str = Field(
        alias="Marble Hill/Rikers Island Alternate Borough Flag"
    )
    list_of_geographic_identifiers_overflow_flag: str = Field(
        alias="List of Geographic Identifiers Overflow Flag"
    )
    strolling_key: StrollingKey = Field(alias="STROLLING KEY")
    building_identification_number_bin_of_input_address_or_nap: str = Field(
        alias="Building Identification Number (BIN) of Input Address or NAP"
    )
    condominium_flag: str = Field(alias="Condominium Flag")
    dof_condominium_identification_number: str = Field(
        alias="DOF Condominium Identification Number"
    )
    condominium_unit_id_number: str = Field(alias="Condominium Unit ID Number")
    condominium_billing_bbl: str = Field(alias="Condominium Billing BBL")
    filler_tax_lot_version_no_billing_bbl: str = Field(
        alias="Filler - Tax Lot Version No. Billing BBL"
    )
    self_check_code_scc_of_billing_bbl: str = Field(
        alias="Self-Check Code (SCC) of Billing BBL"
    )
    low_bbl_of_this_building_s_condominium_units: str = Field(
        alias="Low BBL of this Building's Condominium Units"
    )
    filler_tax_lot_version_no_of_low_bbl: str = Field(
        alias="Filler - Tax Lot Version No. of Low BBL"
    )
    high_bbl_of_this_building_s_condominium_units: str = Field(
        alias="High BBL of this Building's Condominium Units"
    )
    filler_tax_log_version_no_of_high_bbl: str = Field(
        alias="Filler - Tax Log Version No. of High BBL"
    )
    cooperative_id_number: str = Field(alias="Cooperative ID Number")
    sbvp_sanborn_map_identifier: SbvpSanbornMapIdentifier = Field(
        alias="SBVP (SANBORN MAP IDENTIFIER)"
    )
    dcp_commercial_study_area: str = Field(alias="DCP Commercial Study Area")
    tax_map_number_section_volume: str = Field(alias="Tax Map Number Section & Volume")
    reserved_for_tax_map_page_number: str = Field(
        alias="Reserved for Tax Map Page Number"
    )
    x_y_coordinates_of_lot_centroid: str = Field(
        alias="X-Y Coordinates of Lot Centroid"
    )
    business_improvement_district_bid: str = Field(
        alias="Business Improvement District (BID)"
    )
    tpad_bin_status: str = Field(alias="TPAD BIN Status")
    tpad_new_bin: str = Field(alias="TPAD New BIN")
    tpad_new_bin_status: str = Field(alias="TPAD New BIN Status")
    tpad_conflict_flag: str = Field(alias="TPAD Conflict Flag")
    dcp_zoning_map: str = Field(alias="DCP Zoning Map")
    internal_use: str = Field(alias="Internal Use")
    number_of_entries_in_list_of_geographic_identifiers: str = Field(
        alias="Number of Entries in List of Geographic Identifiers"
    )
    list_of_geographic_identifiers: list[ListOfGeographicIdentifiers] = Field(
        alias="LIST OF GEOGRAPHIC IDENTIFIERS"
    )
