from __future__ import annotations
from pydantic import AliasChoices, BaseModel, Field


def validate(obj: BaseModel) -> None | BaseModel:
    if not any(obj.model_dump().values()):
        return None
    else:
        return obj


def FieldStrOrNone(alias: str):
    return Field(alias=alias, default=None)  # TODO - validate, "" -> None


class Bbl(BaseModel):
    bbl: str | None = FieldStrOrNone("BOROUGH BLOCK LOT (BBL)")
    borough_code: str | None = FieldStrOrNone("Borough Code")
    tax_block: str | None = FieldStrOrNone("Tax Block")
    tax_lot: str | None = FieldStrOrNone("Tax Lot")


class UnitSortFormat(BaseModel):
    unit_sort_format: str | None = FieldStrOrNone("UNIT - SORT FORMAT")
    unit_type: str | None = FieldStrOrNone("Unit - Type")
    unit_identifier: str | None = FieldStrOrNone("Unit - Identifier")


## todo remove - these are obviously just bbls.
class LowBblOfThisBuildingsCondominiumUnits(BaseModel):
    low_bbl_of_this_buildings_condominium_units: str | None = FieldStrOrNone(
        "LOW BBL OF THIS BUILDING'S CONDOMINIUM UNITS"
    )
    borough_code: str | None = FieldStrOrNone("Borough Code")
    tax_block: str | None = FieldStrOrNone("Tax Block")
    tax_lot: str | None = FieldStrOrNone("Tax Lot")


class HighBblOfThisBuildingsCondominiumUnits(BaseModel):
    high_bbl_of_this_buildings_condominium_units: str | None = FieldStrOrNone(
        "HIGH BBL OF THIS BUILDING'S CONDOMINIUM UNITS"
    )
    borough_code: str | None = FieldStrOrNone("Borough Code")
    tax_block: str | None = FieldStrOrNone("Tax Block")
    tax_lot: str | None = FieldStrOrNone("Tax Lot")


class SbvpSanbornMapIdentifier(BaseModel):
    sbvp: str | None = FieldStrOrNone("SBVP (SANBORN MAP IDENTIFIER)")
    sanborn_borough_code: str | None = FieldStrOrNone("Sanborn Borough Code")
    volume_number: str | None = FieldStrOrNone("Volume Number")
    volume_number_suffix: str | None = FieldStrOrNone("Volume Number Suffix")
    page_number: str | None = FieldStrOrNone("Page Number")
    page_number_suffix: str | None = FieldStrOrNone("Page Number Suffix")


class ListOfGeographicIdentifiers(BaseModel):
    low_house_number: str | None = FieldStrOrNone("Low House Number")
    high_house_number: str | None = FieldStrOrNone("High House Number")
    borough_code: str | None = FieldStrOrNone("Borough Code")
    five_digit_street_code: str | None = FieldStrOrNone("5-Digit Street Code")
    dcp_preferred_lgc: str | None = FieldStrOrNone(
        "DCP-Preferred Local Group Code (LGC)"
    )
    bin: str | None = FieldStrOrNone("Building Identification Number")
    side_of_street_indicator: str | None = FieldStrOrNone("Side of Street Indicator")
    geographic_identifier_entry_type_code: str | None = FieldStrOrNone(
        "Geographic Identifier Entry Type Code"
    )
    tpad_bin_status: str | None = FieldStrOrNone("TPAD BIN Status")
    street_name: str | None = FieldStrOrNone("Street Name")


class LionKey(BaseModel):
    lion_key: str | None = FieldStrOrNone("LION KEY")
    borough_code: str | None = FieldStrOrNone("Borough Code")
    face_code: str | None = FieldStrOrNone("Face Code")
    sequence_number: str | None = FieldStrOrNone("Sequence Number")


class SpatialXYCoordinatesOfAddress(BaseModel):
    spatial_x_y_coordinates_of_address: str = Field(
        alias="SPATIAL X-Y COORDINATES OF ADDRESS"
    )
    x_coordinate: str = Field(alias="X Coordinate")
    y_coordinate: str = Field(alias="Y Coordinate")


class CommunityDistrict(BaseModel):
    community_district: str | None = FieldStrOrNone("COMMUNITY DISTRICT")
    community_district_borough_code: str | None = FieldStrOrNone(
        "Community District Borough Code"
    )
    community_district_number: str | None = FieldStrOrNone("Community District Number")


class CityServiceInfo(BaseModel):
    health_center_district: str | None = FieldStrOrNone("Health Center District")
    health_area: str | None = FieldStrOrNone("Health Area")
    sanitation_district: str | None = FieldStrOrNone("Sanitation District")
    sanitation_collection_scheduling_section_and_subsection: str | None = (
        FieldStrOrNone("Sanitation Collection Scheduling Section and Subsection")
    )
    sanitation_regular_collection_schedule: str | None = FieldStrOrNone(
        "Sanitation Regular Collection Schedule"
    )
    sanitation_recycling_collection_schedule: str | None = FieldStrOrNone(
        "Sanitation Recycling Collection Schedule"
    )
    dsny_snow_priority_code: str | None = FieldStrOrNone("DSNY Snow Priority Code")
    dsny_organic_recycling_schedule: str | None = FieldStrOrNone(
        "DSNY Organic Recycling Schedule"
    )
    dsny_bulk_pickup_schedule: str | None = FieldStrOrNone("DSNY Bulk Pickup Schedule")
    hurricane_evacuation_zone: str | None = FieldStrOrNone(
        "Hurricane Evacuation Zone (HEZ)"
    )
    police_patrol_borough_command: str | None = FieldStrOrNone(
        "Police Patrol Borough Command"
    )
    police_precinct: str | None = FieldStrOrNone("Police Precinct")
    fire_division: str | None = FieldStrOrNone("Fire Division")
    fire_battalion: str | None = FieldStrOrNone("Fire Battalion")
    fire_company_type: str | None = FieldStrOrNone("Fire Company Type")
    fire_company_number: str | None = FieldStrOrNone("Fire Company Number")
    community_school_district: str | None = FieldStrOrNone("Community School District")
    police_patrol_borough: str | None = FieldStrOrNone("Police Patrol Borough")
    dot_street_light_contractor_area: str | None = FieldStrOrNone(
        "DOT Street Light Contractor Area"
    )

    @property
    def city_service_info(self):
        return CityServiceInfo(self.model_dump())


class CensusInfo(BaseModel):
    borough_of_census_tract: str | None = FieldStrOrNone("Borough of Census Tract")
    census_tract_1990: str | None = FieldStrOrNone("1990 Census Tract")
    census_tract_2000: str | None = FieldStrOrNone("2000 Census Tract")
    census_block_2000: str | None = FieldStrOrNone("2000 Census Block")
    census_block_2000_suffix: str | None = FieldStrOrNone("2000 Census Block Suffix")
    census_tract_2010: str | None = FieldStrOrNone("2010 Census Tract")
    census_block_2010: str | None = FieldStrOrNone("2010 Census Block")
    census_block_2010_suffix: str | None = FieldStrOrNone("2010 Census Block Suffix")
    neighborhood_tabulation_area: str | None = FieldStrOrNone(
        "Neighborhood Tabulation Area (NTA)"
    )
    census_tract_2020: str | None = FieldStrOrNone("2020 Census Tract")
    census_block_2020: str | None = FieldStrOrNone("2020 Census Block")
    census_block_2020_suffix: str | None = FieldStrOrNone("2020 Census Block Suffix")
    neighborhood_tabulation_area_2020: str | None = FieldStrOrNone(
        "2020 Neighborhood Tabulation Area (NTA)"
    )
    community_district_tabulation_area_2020: str | None = FieldStrOrNone(
        "2020 Community District Tabulation Area (CDTA)"
    )
    puma_code: str | None = FieldStrOrNone("PUMA Code")

    @property
    def census_info(self):
        return CityServiceInfo(self.model_dump())


class PoliticalInfo(BaseModel):
    election_district: str | None = FieldStrOrNone("Election District")
    assembly_district: str | None = FieldStrOrNone("Assembly District")
    split_election_district_flag: str | None = FieldStrOrNone(
        "Split Election District Flag"
    )
    congressional_district: str | None = FieldStrOrNone("Congressional District")
    state_senatorial_district: str | None = FieldStrOrNone("State Senatorial District")
    civil_court_district: str | None = FieldStrOrNone("Civil Court District")
    city_council_district: str | None = FieldStrOrNone("City Council District")


class BlockFace(BaseModel):  # 1, 1E
    pass


class GeoSupportReturn(BaseModel):
    reason_code: str | None = FieldStrOrNone("Reason Code")
    reason_code_qualifier: str | None = FieldStrOrNone("Reason Code Qualifier")
    warning_code: str | None = FieldStrOrNone("Warning Code")
    geosupport_return_code: str | None = FieldStrOrNone("Geosupport Return Code (GRC)")
    message: str | None = FieldStrOrNone("Message")
    reason_code_2: str | None = FieldStrOrNone("Reason Code 2")
    reason_code_qualifier_2: str | None = FieldStrOrNone("Reason Code Qualifier 2")
    warning_code_2: str | None = FieldStrOrNone("Warning Code 2")
    geosupport_return_code_2: str | None = FieldStrOrNone(
        "Geosupport Return Code 2 (GRC 2)"
    )
    message_2: str | None = FieldStrOrNone("Message 2")


class Result1(
    CityServiceInfo, CensusInfo, PoliticalInfo, GeoSupportReturn, extra="forbid"
):
    first_borough_name: str | None = FieldStrOrNone("First Borough Name")
    house_number_display_format: str | None = FieldStrOrNone(
        "House Number - Display Format"
    )
    house_number_sort_format: str | None = FieldStrOrNone("House Number - Sort Format")
    b10sc_first_borough_and_street_code: str | None = FieldStrOrNone(
        "B10SC - First Borough and Street Code"
    )
    first_street_name_normalized: str | None = FieldStrOrNone(
        "First Street Name Normalized"
    )
    b10sc_second_borough_and_street_code: str | None = FieldStrOrNone(
        "B10SC - Second Borough and Street Code"
    )
    second_street_name_normalized: str | None = FieldStrOrNone(
        "Second Street Name Normalized"
    )
    b10sc_third_borough_and_street_code: str | None = FieldStrOrNone(
        "B10SC - Third Borough and Street Code"
    )
    third_street_name_normalized: str | None = FieldStrOrNone(
        "Third Street Name Normalized"
    )
    bbl: Bbl = Field(alias="BOROUGH BLOCK LOT (BBL)")
    filler_for_tax_lot_version_number: str | None = FieldStrOrNone(
        "Filler for Tax Lot Version Number"
    )
    low_house_number_display_format: str | None = FieldStrOrNone(
        "Low House Number - Display Format"
    )
    low_house_number_sort_format: str | None = FieldStrOrNone(
        "Low House Number - Sort Format"
    )
    bin: str | None = FieldStrOrNone("Building Identification Number (BIN)")
    street_attribute_indicators: str | None = FieldStrOrNone(
        "Street Attribute Indicators"
    )
    node_number: str | None = FieldStrOrNone("Node Number")
    unit_sort_format: UnitSortFormat = Field(alias="UNIT - SORT FORMAT")
    unit_display_format: str | None = FieldStrOrNone("Unit - Display Format")
    nin: str | None = FieldStrOrNone("NIN")
    street_attribute_indicator: str | None = FieldStrOrNone(
        "Street Attribute Indicator"
    )
    number_of_street_codes_and_street_names_in_list: str | None = FieldStrOrNone(
        "Number of Street Codes and Street Names in List"
    )
    list_of_street_codes: list = Field(alias="List of Street Codes")
    list_of_street_names: list = Field(alias="List of Street Names")
    continuous_parity_duplicate_address_indicator: str | None = FieldStrOrNone(
        "Continuous Parity Indicator/Duplicate Address Indicator"
    )
    low_house_number_of_block_face: str | None = FieldStrOrNone(
        "Low House Number of Block Face"
    )
    high_house_number_of_block_face: str | None = FieldStrOrNone(
        "High House Number of Block Face"
    )
    dcp_preferred_lgc: str | None = FieldStrOrNone("DCP Preferred LGC")
    no_of_cross_streets_at_low_address_end: str | None = FieldStrOrNone(
        "No. of Cross Streets at Low Address End"
    )
    list_of_cross_streets_at_low_address_end: str | None = FieldStrOrNone(
        "List of Cross Streets at Low Address End"
    )
    no_of_cross_streets_at_high_address_end: str | None = FieldStrOrNone(
        "No. of Cross Streets at High Address End"
    )
    list_of_cross_streets_at_high_address_end: str | None = FieldStrOrNone(
        "List of Cross Streets at High Address End"
    )
    lion_key: LionKey = Field(alias="LION KEY")
    special_address_generated_record_flag: str | None = FieldStrOrNone(
        "Special Address Generated Record Flag"
    )
    side_of_street_indicator: str | None = FieldStrOrNone("Side of Street Indicator")
    segment_length_in_feet: str | None = FieldStrOrNone("Segment Length in Feet")
    spatial_x_y_coordinates_of_address: SpatialXYCoordinatesOfAddress = Field(
        alias="SPATIAL X-Y COORDINATES OF ADDRESS"
    )
    reserved_for_possible_z_coordinate: str | None = FieldStrOrNone(
        "Reserved for Possible Z Coordinate"
    )
    community_development_eligibility_indicator: str | None = FieldStrOrNone(
        "Community Development Eligibility Indicator"
    )
    marble_hill_rikers_island_alternative_borough_flag: str | None = FieldStrOrNone(
        "Marble Hill/Rikers Island Alternative Borough Flag"
    )
    community_district: CommunityDistrict | None = Field(alias="COMMUNITY DISTRICT")
    atomic_polygon: str | None = FieldStrOrNone("Atomic Polygon")
    zip_code: str | None = FieldStrOrNone("ZIP Code")
    feature_type_code: str | None = FieldStrOrNone("Feature Type Code")
    segment_type_code: str | None = FieldStrOrNone("Segment Type Code")
    alley_or_cross_street_list_flag: str | None = FieldStrOrNone(
        "Alley or Cross Street List Flag"
    )
    coincidence_segment_count: str | None = FieldStrOrNone("Coincidence Segment Count")
    underlying_address_number_on_true_street: str | None = FieldStrOrNone(
        "Underlying Address Number on True Street"
    )
    underlying_b7sc_of_true_street: str | None = FieldStrOrNone(
        "Underlying B7SC of True Street"
    )
    segment_identifier: str | None = FieldStrOrNone("Segment Identifier")
    curve_flag: str | None = FieldStrOrNone("Curve Flag")


class Result1A(GeoSupportReturn, extra="forbid"):
    first_borough_name: str | None = FieldStrOrNone("First Borough Name")
    house_number_display_format: str | None = FieldStrOrNone(
        "House Number - Display Format"
    )
    house_number_sort_format: str | None = FieldStrOrNone("House Number - Sort Format")
    b10sc_first_borough_and_street_code: str | None = FieldStrOrNone(
        "B10SC - First Borough and Street Code"
    )
    first_street_name_normalized: str | None = FieldStrOrNone(
        "First Street Name Normalized"
    )
    b10sc_second_borough_and_street_code: str | None = FieldStrOrNone(
        "B10SC - Second Borough and Street Code"
    )
    second_street_name_normalized: str | None = FieldStrOrNone(
        "Second Street Name Normalized"
    )
    b10sc_third_borough_and_street_code: str | None = FieldStrOrNone(
        "B10SC - Third Borough and Street Code"
    )
    third_street_name_normalized: str | None = FieldStrOrNone(
        "Third Street Name Normalized"
    )
    bbl: Bbl | None = Field(alias="BOROUGH BLOCK LOT (BBL)")
    filler_for_tax_lot_version_number: str | None = FieldStrOrNone(
        "Filler for Tax Lot Version Number"
    )
    low_house_number_display_format: str | None = FieldStrOrNone(
        "Low House Number - Display Format"
    )
    low_house_number_sort_format: str | None = FieldStrOrNone(
        "Low House Number - Sort Format"
    )
    bin: str | None = FieldStrOrNone("Building Identification Number (BIN)")
    street_attribute_indicators: str | None = FieldStrOrNone(
        "Street Attribute Indicators"
    )
    node_number: str | None = FieldStrOrNone("Node Number")
    unit_sort_format: UnitSortFormat | None = Field(alias="UNIT - SORT FORMAT")
    unit_display_format: str | None = FieldStrOrNone("Unit - Display Format")
    nin: str | None = FieldStrOrNone("NIN")
    street_attribute_indicator: str | None = FieldStrOrNone(
        "Street Attribute Indicator"
    )
    number_of_street_codes_and_street_names_in_list: str | None = FieldStrOrNone(
        "Number of Street Codes and Street Names in List"
    )
    list_of_street_codes: list = Field(alias="List of Street Codes")
    list_of_street_names: list = Field(alias="List of Street Names")
    continuous_parity_duplicate_address_indicator: str | None = FieldStrOrNone(
        "Continuous Parity Indicator /Duplicate Address Indicator"
    )
    low_house_number_of_defining_address_range: str | None = FieldStrOrNone(
        "Low House Number of Defining Address Range"
    )
    rpad_self_check_code_for_bbl: str | None = FieldStrOrNone(
        "RPAD Self-Check Code (SCC) for BBL"
    )
    rpad_building_classification_code: str | None = FieldStrOrNone(
        "RPAD Building Classification Code"
    )
    corner_code: str | None = FieldStrOrNone("Corner Code")
    number_of_existing_structures_on_lot: str | None = FieldStrOrNone(
        "Number of Existing Structures on Lot"
    )
    number_of_street_frontages_of_lot: str | None = FieldStrOrNone(
        "Number of Street Frontages of Lot"
    )
    interior_lot_flag: str | None = FieldStrOrNone("Interior Lot Flag")
    vacant_lot_flag: str | None = FieldStrOrNone("Vacant Lot Flag")
    irregularly_shaped_lot_flag: str | None = FieldStrOrNone(
        "Irregularly-Shaped Lot Flag"
    )
    marble_hill_rikers_island_alternate_borough_flag: str | None = FieldStrOrNone(
        "Marble Hill/Rikers Island Alternate Borough Flag"
    )
    lgi_overflow_flag: str | None = FieldStrOrNone(
        "List of Geographic Identifiers (LGI) Overflow Flag"
    )
    strolling_key: str | None = FieldStrOrNone("STROLLING KEY")
    borough: str | None = FieldStrOrNone("Borough")
    five_digit_street_code_of_on_street: str | None = FieldStrOrNone(
        "5-Digit Street Code of 'On' Street"
    )
    side_of_street_indicator: str | None = FieldStrOrNone("Side of Street Indicator")
    high_house_number_sort_format: str | None = FieldStrOrNone(
        "High House Number - Sort Format"
    )
    reserved_for_internal_use: str | None = FieldStrOrNone("Reserved for Internal Use")
    bin_of_input_address_or_nap: str | None = FieldStrOrNone(
        "Building Identification Number (BIN) of Input Address or NAP"
    )
    condominium_flag: str | None = FieldStrOrNone("Condominium Flag")
    dof_condominium_identification_number: str | None = FieldStrOrNone(
        "DOF Condominium Identification Number"
    )
    condominium_unit_id_number: str | None = FieldStrOrNone(
        "Condominium Unit ID Number"
    )
    condominium_billing_bbl: str | None = FieldStrOrNone("Condominium Billing BBL")
    filler_tax_lot_version_no_for_billing_bbl: str | None = FieldStrOrNone(
        "Filler - Tax Lot Version No. for Billing BBL"
    )
    self_check_code_of_billing_bbl: str | None = FieldStrOrNone(
        "Self-Check Code (SCC) of Billing BBL"
    )
    low_bbl_of_this_buildings_condominium_units: (
        LowBblOfThisBuildingsCondominiumUnits | None
    ) = Field(
        alias="LOW BBL OF THIS BUILDING'S CONDOMINIUM UNITS"  # todo validate or none
    )
    filler_for_tax_lot_version_no_of_low_bbl: str | None = FieldStrOrNone(
        "Filler for Tax Lot Version No. of Low BBL"
    )
    high_bbl_of_this_buildings_condominium_units: (
        HighBblOfThisBuildingsCondominiumUnits | None
    ) = Field(
        alias="HIGH BBL OF THIS BUILDING'S CONDOMINIUM UNITS"  # odo validate or none
    )
    filler_for_tax_lot_version_no_of_high_bbl: str | None = FieldStrOrNone(
        "Filler for Tax Lot Version No. of High BBL"
    )
    cooperative_id_number: str | None = FieldStrOrNone("Cooperative ID Number")
    sbvp_sanborn_map_identifier: SbvpSanbornMapIdentifier | None = Field(
        alias="SBVP (SANBORN MAP IDENTIFIER)"  # todo
    )
    dcp_commercial_study_area: str | None = FieldStrOrNone("DCP Commercial Study Area")
    tax_map_number_section_volume: str | None = FieldStrOrNone(
        "Tax Map Number Section & Volume"
    )
    reserved_for_tax_map_page_number: str | None = FieldStrOrNone(
        "Reserved for Tax Map Page Number"
    )
    latitude: str | None = FieldStrOrNone("Latitude")
    longitude: str | None = FieldStrOrNone("Longitude")
    x_y_coordinates_of_tax_lot_centroid_internal_to_lot: str | None = FieldStrOrNone(
        "X-Y Coordinates of Tax Lot Centroid (Internal to Lot)"
    )
    business_improvement_district: str | None = FieldStrOrNone(
        "Business Improvement District (BID)"
    )
    tpad_bin_status_for_dm_job: str | None = FieldStrOrNone(
        "TPAD BIN Status (for DM job)"
    )
    tpad_new_bin: str | None = FieldStrOrNone("TPAD New BIN")
    tpad_new_bin_status: str | None = FieldStrOrNone("TPAD New BIN Status")
    tpad_conflict_flag: str | None = FieldStrOrNone("TPAD Conflict Flag")
    dcp_zoning_map: str | None = FieldStrOrNone("DCP Zoning Map")
    list_of_4_lgcs: str | None = FieldStrOrNone("List of 4 LGCs")
    number_of_entries_in_list_of_geographic_identifiers: str | None = FieldStrOrNone(
        "Number of Entries in List of Geographic Identifiers"
    )
    list_of_geographic_identifiers: list[ListOfGeographicIdentifiers] = Field(
        alias="LIST OF GEOGRAPHIC IDENTIFIERS"
    )


class Result1E(
    CityServiceInfo, CensusInfo, PoliticalInfo, GeoSupportReturn, extra="forbid"
):
    first_borough_name: str | None = FieldStrOrNone("First Borough Name")
    house_number_display_format: str | None = FieldStrOrNone(
        "House Number - Display Format"
    )
    house_number_sort_format: str | None = FieldStrOrNone("House Number - Sort Format")
    b10sc_first_borough_and_street_code: str | None = FieldStrOrNone(
        "B10SC - First Borough and Street Code"
    )
    first_street_name_normalized: str | None = FieldStrOrNone(
        "First Street Name Normalized"
    )
    b10sc_second_borough_and_street_code: str | None = FieldStrOrNone(
        "B10SC - Second Borough and Street Code"
    )
    second_street_name_normalized: str | None = FieldStrOrNone(
        "Second Street Name Normalized"
    )
    b10sc_third_borough_and_street_code: str | None = FieldStrOrNone(
        "B10SC - Third Borough and Street Code"
    )
    third_street_name_normalized: str | None = FieldStrOrNone(
        "Third Street Name Normalized"
    )
    bbl: Bbl | None = Field(alias="BOROUGH BLOCK LOT (BBL)")
    filler_for_tax_lot_version_number: str | None = FieldStrOrNone(
        "Filler for Tax Lot Version Number"
    )
    low_house_number_display_format: str | None = FieldStrOrNone(
        "Low House Number - Display Format"
    )
    low_house_number_sort_format: str | None = FieldStrOrNone(
        "Low House Number - Sort Format"
    )
    building_identification_number_bin: str | None = FieldStrOrNone(
        "Building Identification Number (BIN)"
    )
    street_attribute_indicators: str | None = FieldStrOrNone(
        "Street Attribute Indicators"
    )
    node_number: str | None = FieldStrOrNone("Node Number")
    unit_sort_format: UnitSortFormat | None = Field(alias="UNIT - SORT FORMAT")
    unit_display_format: str | None = FieldStrOrNone("Unit - Display Format")
    nin: str | None = FieldStrOrNone("NIN")
    street_attribute_indicator: str | None = FieldStrOrNone(
        "Street Attribute Indicator"
    )
    number_of_street_codes_and_street_names_in_list: str | None = FieldStrOrNone(
        "Number of Street Codes and Street Names in List"
    )
    list_of_street_codes: list = Field(alias="List of Street Codes")
    list_of_street_names: list = Field(alias="List of Street Names")
    continuous_parity_indicator_duplicate_address_indicator: str | None = (
        FieldStrOrNone("Continuous Parity Indicator/Duplicate Address Indicator")
    )
    low_house_number_of_block_face: str | None = FieldStrOrNone(
        "Low House Number of Block Face"
    )
    high_house_number_of_block_face: str | None = FieldStrOrNone(
        "High House Number of Block Face"
    )
    dcp_preferred_lgc: str | None = FieldStrOrNone("DCP Preferred LGC")
    no_of_cross_streets_at_low_address_end: str | None = FieldStrOrNone(
        "No. of Cross Streets at Low Address End"
    )
    list_of_cross_streets_at_low_address_end: str | None = FieldStrOrNone(
        "List of Cross Streets at Low Address End"
    )
    no_of_cross_streets_at_high_address_end: str | None = FieldStrOrNone(
        "No. of Cross Streets at High Address End"
    )
    list_of_cross_streets_at_high_address_end: str | None = FieldStrOrNone(
        "List of Cross Streets at High Address End"
    )
    lion_key: LionKey | None = Field(alias="LION KEY")
    special_address_generated_record_flag: str | None = FieldStrOrNone(
        "Special Address Generated Record Flag"
    )
    side_of_street_indicator: str | None = FieldStrOrNone("Side of Street Indicator")
    segment_length_in_feet: str | None = FieldStrOrNone("Segment Length in Feet")
    spatial_x_y_coordinates_of_address: SpatialXYCoordinatesOfAddress | None = Field(
        "SPATIAL X-Y COORDINATES OF ADDRESS"
    )
    reserved_for_possible_z_coordinate: str | None = FieldStrOrNone(
        "Reserved for Possible Z Coordinate"
    )
    community_development_eligibility_indicator: str | None = FieldStrOrNone(
        "Community Development Eligibility Indicator"
    )
    marble_hill_rikers_island_alternative_borough_flag: str | None = FieldStrOrNone(
        "Marble Hill/Rikers Island Alternative Borough Flag"
    )
    community_district: CommunityDistrict | None = Field(alias="COMMUNITY DISTRICT")
    zip_code: str | None = FieldStrOrNone("ZIP Code")
    community_school_district: str | None = FieldStrOrNone("Community School District")
    atomic_polygon: str | None = FieldStrOrNone("Atomic Polygon")
    feature_type_code: str | None = FieldStrOrNone("Feature Type Code")
    segment_type_code: str | None = FieldStrOrNone("Segment Type Code")
    alley_or_cross_street_list_flag: str | None = FieldStrOrNone(
        "Alley or Cross Street List Flag"
    )
    coincidence_segment_count: str | None = FieldStrOrNone("Coincidence Segment Count")
    underlying_address_number_on_true_street: str | None = FieldStrOrNone(
        "Underlying Address Number on True Street"
    )
    underlying_b7_sc_of_true_street: str | None = FieldStrOrNone(
        "Underlying B7SC of True Street"
    )
    segment_identifier: str | None = FieldStrOrNone("Segment Identifier")
    curve_flag: str | None = FieldStrOrNone("Curve Flag")


class SpatialCoordinatesOfSegment(BaseModel, extra="forbid"):
    spatial_coordinates_of_segment: str | None = FieldStrOrNone(
        "SPATIAL COORDINATES OF SEGMENT"
    )
    x_coordinate_low_address_end: str | None = FieldStrOrNone(
        "X Coordinate, Low Address End"
    )
    y_coordinate_low_address_end: str | None = FieldStrOrNone(
        "Y Coordinate, Low Address End"
    )
    z_coordinate_low_address_end: str | None = FieldStrOrNone(
        "Z Coordinate, Low Address End"
    )
    x_coordinate_high_address_end: str | None = FieldStrOrNone(
        "X Coordinate, High Address End"
    )
    y_coordinate_high_address_end: str | None = FieldStrOrNone(
        "Y Coordinate, High Address End"
    )
    z_coordinate_high_address_end: str | None = FieldStrOrNone(
        "Z Coordinate, High Address End"
    )


class SpatialCoordinatesOfCenterOfCurvature(BaseModel, extra="forbid"):
    spatial_coordinates_of_center_of_curvature: str | None = FieldStrOrNone(
        "SPATIAL COORDINATES OF CENTER OF CURVATURE"
    )
    x_coordinate: str | None = FieldStrOrNone("X Coordinate")
    y_coordinate: str | None = FieldStrOrNone("Y Coordinate")
    z_coordinate: str | None = FieldStrOrNone("Z Coordinate")


class SpatialCoordinatesOfActualSegment(BaseModel, extra="forbid"):
    spatial_coordinates_of_actual_segment: str | None = FieldStrOrNone(
        "SPATIAL COORDINATES OF ACTUAL SEGMENT"
    )
    x_coordinate_low_address_end: str | None = FieldStrOrNone(
        "X Coordinate, Low Address End"
    )
    y_coordinate_low_address_end: str | None = FieldStrOrNone(
        "Y Coordinate, Low Address End"
    )
    z_coordinate_low_address_end: str | None = FieldStrOrNone(
        "Z Coordinate, Low Address End"
    )
    x_coordinate_high_address_end: str | None = FieldStrOrNone(
        "X Coordinate, High Address End"
    )
    y_coordinate_high_address_end: str | None = FieldStrOrNone(
        "Y Coordinate, High Address End"
    )
    z_coordinate_high_address_end: str | None = FieldStrOrNone(
        "Z Coordinate, High Address End"
    )


class StrollingKey(BaseModel, extra="forbid"):
    strolling_key: str | None = FieldStrOrNone("STROLLING KEY")
    borough: str | None = FieldStrOrNone("Borough")
    five_digit_street_code_of_on_street: str | None = FieldStrOrNone(
        "5-Digit Street Code of ON- Street"
    )
    side_of_street_indicator: str | None = FieldStrOrNone("Side of Street Indicator")
    high_house_number: str | None = FieldStrOrNone("High House Number")


class Result1B(
    CityServiceInfo, CensusInfo, PoliticalInfo, GeoSupportReturn, extra="forbid"
):
    first_borough_name: str | None = FieldStrOrNone("First Borough Name")
    house_number_display_format: str | None = FieldStrOrNone(
        "House Number - Display Format"
    )
    house_number_sort_format: str | None = FieldStrOrNone("House Number - Sort Format")
    b10_sc_first_borough_and_street_code: str | None = FieldStrOrNone(
        "B10SC - First Borough and Street Code"
    )
    first_street_name_normalized: str | None = FieldStrOrNone(
        "First Street Name Normalized"
    )
    b10_sc_second_borough_and_street_code: str | None = FieldStrOrNone(
        "B10SC - Second Borough and Street Code"
    )
    second_street_name_normalized: str | None = FieldStrOrNone(
        "Second Street Name Normalized"
    )
    b10_sc_third_borough_and_street_code: str | None = FieldStrOrNone(
        "B10SC - Third Borough and Street Code"
    )
    third_street_name_normalized: str | None = FieldStrOrNone(
        "Third Street Name Normalized"
    )
    borough_block_lot_bbl: Bbl | None = Field(alias="BOROUGH BLOCK LOT (BBL)")
    filler_for_tax_lot_version_number: str | None = FieldStrOrNone(
        "Filler for Tax Lot Version Number"
    )
    low_house_number_display_format: str | None = FieldStrOrNone(
        "Low House Number - Display Format"
    )
    low_house_number_sort_format: str | None = FieldStrOrNone(
        "Low House Number - Sort Format"
    )
    bin: str | None = FieldStrOrNone("Building Identification Number (BIN)")
    street_attribute_indicators: str | None = FieldStrOrNone(
        "Street Attribute Indicators"
    )
    node_number: str | None = FieldStrOrNone("Node Number")
    unit_sort_format: UnitSortFormat | None = Field(alias="UNIT - SORT FORMAT")
    unit_display_format: str | None = FieldStrOrNone("Unit - Display Format")
    nin: str | None = FieldStrOrNone("NIN")
    street_attribute_indicator: str | None = FieldStrOrNone(
        "Street Attribute Indicator"
    )
    number_of_street_codes_and_street_names_in_list: str | None = FieldStrOrNone(
        "Number of Street Codes and Street Names in List"
    )
    list_of_street_codes: list = Field(alias="List of Street Codes")
    list_of_street_names: list = Field(alias="List of Street Names")
    continuous_parity_indicator_duplicate_address_indicator: str | None = (
        FieldStrOrNone("Continuous Parity Indicator/Duplicate Address Indicator")
    )
    low_house_number_of_block_face: str | None = FieldStrOrNone(
        "Low House Number of Block Face"
    )
    high_house_number_of_block_face: str | None = FieldStrOrNone(
        "High House Number of Block Face"
    )
    dcp_preferred_lgc: str | None = FieldStrOrNone("DCP Preferred LGC")
    number_of_cross_streets_at_low_address_end: str | None = FieldStrOrNone(
        "Number of Cross Streets at Low Address End"
    )
    list_of_cross_streets_at_low_address_end: str | None = FieldStrOrNone(
        "List of Cross Streets at Low Address End"
    )
    number_of_cross_streets_at_high_address_end: str | None = FieldStrOrNone(
        "Number of Cross Streets at High Address End"
    )
    list_of_cross_streets_at_high_address_end: str | None = FieldStrOrNone(
        "List of Cross Streets at High Address End"
    )
    lion_key: LionKey | None = Field(alias="LION KEY")
    special_address_generated_record_flag: str | None = FieldStrOrNone(
        "Special Address Generated Record Flag"
    )
    side_of_street_indicator: str | None = FieldStrOrNone("Side of Street Indicator")
    segment_length_in_feet: str | None = FieldStrOrNone("Segment Length in Feet")
    spatial_x_y_coordinates_of_address: str | None = FieldStrOrNone(
        "Spatial X-Y Coordinates of Address"
    )
    reserved_for_possible_z_coordinate: str | None = FieldStrOrNone(
        "Reserved for Possible Z Coordinate"
    )
    community_development_eligibility_indicator: str | None = FieldStrOrNone(
        "Community Development Eligibility Indicator"
    )
    marble_hill_rikers_island_alternative_borough_flag: str | None = FieldStrOrNone(
        "Marble Hill/Rikers Island Alternative Borough Flag"
    )
    dot_street_light_contractor_area: str | None = FieldStrOrNone(
        "DOT Street Light Contractor Area"
    )
    community_district: CommunityDistrict | None = Field(alias="COMMUNITY DISTRICT")
    zip_code: str | None = FieldStrOrNone("ZIP Code")
    community_school_district: str | None = FieldStrOrNone("Community School District")
    atomic_polygon: str | None = FieldStrOrNone("Atomic Polygon")
    feature_type_code: str | None = FieldStrOrNone("Feature Type Code")
    segment_type_code: str | None = FieldStrOrNone("Segment Type Code")
    alley_or_cross_street_list_flag: str | None = FieldStrOrNone(
        "Alley or Cross Street List Flag"
    )
    coincidence_segment_count: str | None = FieldStrOrNone("Coincidence Segment Count")
    underlying_address_number_for_na_ps: str | None = FieldStrOrNone(
        "Underlying Address Number for NAPs"
    )
    underlying_b7_sc: str | None = FieldStrOrNone("Underlying B7SC")
    segment_identifier: str | None = FieldStrOrNone("Segment Identifier")
    curve_flag: str | None = FieldStrOrNone("Curve Flag")
    list_of_4_lg_cs: str | None = FieldStrOrNone("List of 4 LGCs")
    boe_lgc_pointer: str | None = FieldStrOrNone("BOE LGC Pointer")
    segment_azimuth: str | None = FieldStrOrNone("Segment Azimuth")
    segment_orientation: str | None = FieldStrOrNone("Segment Orientation")
    spatial_coordinates_of_segment: SpatialCoordinatesOfSegment | None = Field(
        alias="SPATIAL COORDINATES OF SEGMENT"
    )
    spatial_coordinates_of_center_of_curvature: (
        SpatialCoordinatesOfCenterOfCurvature | None
    ) = Field(alias="SPATIAL COORDINATES OF CENTER OF CURVATURE")
    radius_of_circle: str | None = FieldStrOrNone("Radius of Circle")
    secant_location_related_to_curve: str | None = FieldStrOrNone(
        "Secant Location Related to Curve"
    )
    angle_to_from_node_beta_value: str | None = FieldStrOrNone(
        "Angle to From Node - Beta Value"
    )
    angle_to_to_node_alpha_value: str | None = FieldStrOrNone(
        "Angle to To Node - Alpha Value"
    )
    from_lion_node_id: str | None = FieldStrOrNone("From LION Node ID")
    to_lion_node_id: str | None = FieldStrOrNone("To LION Node ID")
    lion_key_for_vanity_address: str | None = FieldStrOrNone(
        "LION Key for Vanity Address"
    )
    side_of_street_of_vanity_address: str | None = FieldStrOrNone(
        "Side of Street of Vanity Address"
    )
    split_low_house_number: str | None = FieldStrOrNone("Split Low House Number")
    traffic_direction: str | None = FieldStrOrNone("Traffic Direction")
    turn_restrictions: str | None = FieldStrOrNone("Turn Restrictions")
    fraction_for_curve_calculation: str | None = FieldStrOrNone(
        "Fraction for Curve Calculation"
    )
    roadway_type: str | None = FieldStrOrNone("Roadway Type")
    physical_id: str | None = FieldStrOrNone("Physical ID")
    generic_id: str | None = FieldStrOrNone("Generic ID")
    nypd_id: str | None = FieldStrOrNone("NYPD ID")
    fdny_id: str | None = FieldStrOrNone("FDNY ID")
    bike_lane_2: str | None = FieldStrOrNone("Bike Lane 2")
    bike_traffic_direction: str | None = FieldStrOrNone("Bike Traffic Direction")
    street_status: str | None = FieldStrOrNone("Street Status")
    street_width: str | None = FieldStrOrNone("Street Width")
    street_width_irregular: str | None = FieldStrOrNone("Street Width Irregular")
    bike_lane: str | None = FieldStrOrNone("Bike Lane")
    federal_classification_code: str | None = FieldStrOrNone(
        "Federal Classification Code"
    )
    right_of_way_type: str | None = FieldStrOrNone("Right Of Way Type")
    list_of_second_set_of_5_lg_cs: str | None = FieldStrOrNone(
        "List of Second Set of 5 LGCs"
    )
    legacy_segment_id: str | None = FieldStrOrNone("Legacy Segment ID")
    from_preferred_lg_cs_first_set_of_5: str | None = FieldStrOrNone(
        "From Preferred LGCs First Set of 5"
    )
    to_preferred_lg_cs_first_set_of_5: str | None = FieldStrOrNone(
        "To Preferred LGCs First Set of 5"
    )
    from_preferred_lg_cs_second_set_of_5: str | None = FieldStrOrNone(
        "From Preferred LGCs Second Set of 5"
    )
    to_preferred_lg_cs_second_set_of_5: str | None = FieldStrOrNone(
        "To Preferred LGCs Second Set of 5"
    )
    no_cross_street_calculation_flag: str | None = FieldStrOrNone(
        "No Cross Street Calculation Flag"
    )
    individual_segment_length: str | None = FieldStrOrNone("Individual Segment Length")
    nta_name: str | None = FieldStrOrNone("NTA Name")
    usps_preferred_city_name: str | None = FieldStrOrNone("USPS Preferred City Name")
    latitude: str | None = FieldStrOrNone("Latitude")
    longitude: str | None = FieldStrOrNone("Longitude")
    from_actual_segment_node_id: str | None = FieldStrOrNone(
        "From Actual Segment Node ID"
    )
    to_actual_segment_node_id: str | None = FieldStrOrNone("To Actual Segment Node ID")
    spatial_coordinates_of_actual_segment: SpatialCoordinatesOfActualSegment | None = (
        Field(alias="SPATIAL COORDINATES OF ACTUAL SEGMENT")
    )
    blockface_id: str | None = FieldStrOrNone("Blockface ID")
    number_of_travel_lanes_on_the_street: str | None = FieldStrOrNone(
        "Number of Travel Lanes on the Street"
    )
    number_of_parking_lanes_on_the_street: str | None = FieldStrOrNone(
        "Number of Parking Lanes on the Street"
    )
    number_of_total_lanes_on_the_street: str | None = FieldStrOrNone(
        "Number of Total Lanes on the Street"
    )
    street_width_maximum: str | None = FieldStrOrNone("Street Width Maximum")
    speed_limit: str | None = FieldStrOrNone("Speed Limit")
    police_sector: str | None = FieldStrOrNone("Police Sector")
    police_service_area: str | None = FieldStrOrNone("Police Service Area")
    truck_route_type: str | None = FieldStrOrNone("Truck Route Type")
    filler: str | None = FieldStrOrNone("Filler")
    return_code: str | None = FieldStrOrNone("Return Code")
    no_of_cross_streets_at_high_address_end: str | None = FieldStrOrNone(
        "No. of Cross Streets at High Address End"
    )
    list_of_cross_street_names_at_low_address_end: str | None = FieldStrOrNone(
        "List of Cross Street Names at Low Address End"
    )
    list_of_cross_street_names_at_high_address_end: str | None = FieldStrOrNone(
        "List of Cross Street Names at High Address End"
    )
    boe_preferred_b7_sc: str | None = FieldStrOrNone("BOE Preferred B7SC")
    boe_preferred_street_name: str | None = FieldStrOrNone("BOE Preferred Street Name")
    continuous_parity_indicator_duplicate_address_indicator: str | None = Field(
        validation_alias=AliasChoices(
            "Continuous Parity Indicator / Duplicate Address Indicator",
            "Continuous Parity Indicator /Duplicate Address Indicator",
            "Continuous Parity Indicator/Duplicate Address Indicator",
        )
    )
    continuous_parity_indicator_duplicate_address_indicator: str | None = (
        FieldStrOrNone("Continuous Parity Indicator /Duplicate Address Indicator")
    )
    low_house_number_of_defining_address_range: str | None = FieldStrOrNone(
        "Low House Number of Defining Address Range"
    )
    rpad_self_check_code_scc_for_bbl: str | None = FieldStrOrNone(
        "RPAD Self-Check Code (SCC) for BBL"
    )
    rpad_building_classification_code: str | None = FieldStrOrNone(
        "RPAD Building Classification Code"
    )
    corner_code: str | None = FieldStrOrNone("Corner Code")
    number_of_existing_structures_on_lot: str | None = FieldStrOrNone(
        "Number of Existing Structures on Lot"
    )
    number_of_street_frontages_of_lot: str | None = FieldStrOrNone(
        "Number of Street Frontages of Lot"
    )
    interior_lot_flag: str | None = FieldStrOrNone("Interior Lot Flag")
    vacant_lot_flag: str | None = FieldStrOrNone("Vacant Lot Flag")
    irregularly_shaped_lot_flag: str | None = FieldStrOrNone(
        "Irregularly-Shaped Lot Flag"
    )
    marble_hill_rikers_island_alternate_borough_flag: str | None = FieldStrOrNone(
        "Marble Hill/Rikers Island Alternate Borough Flag"
    )
    list_of_geographic_identifiers_overflow_flag: str | None = FieldStrOrNone(
        "List of Geographic Identifiers Overflow Flag"
    )
    strolling_key: StrollingKey | None = Field(alias="STROLLING KEY")
    building_identification_number_bin_of_input_address_or_nap: str | None = (
        FieldStrOrNone("Building Identification Number (BIN) of Input Address or NAP")
    )
    condominium_flag: str | None = FieldStrOrNone("Condominium Flag")
    dof_condominium_identification_number: str | None = FieldStrOrNone(
        "DOF Condominium Identification Number"
    )
    condominium_unit_id_number: str | None = FieldStrOrNone(
        "Condominium Unit ID Number"
    )
    condominium_billing_bbl: str | None = FieldStrOrNone("Condominium Billing BBL")
    filler_tax_lot_version_no_billing_bbl: str | None = FieldStrOrNone(
        "Filler - Tax Lot Version No. Billing BBL"
    )
    self_check_code_scc_of_billing_bbl: str | None = FieldStrOrNone(
        "Self-Check Code (SCC) of Billing BBL"
    )
    low_bbl_of_this_building_s_condominium_units: str | None = FieldStrOrNone(
        "Low BBL of this Building's Condominium Units"
    )
    filler_tax_lot_version_no_of_low_bbl: str | None = FieldStrOrNone(
        "Filler - Tax Lot Version No. of Low BBL"
    )
    high_bbl_of_this_building_s_condominium_units: str | None = FieldStrOrNone(
        "High BBL of this Building's Condominium Units"
    )
    filler_tax_log_version_no_of_high_bbl: str | None = FieldStrOrNone(
        "Filler - Tax Log Version No. of High BBL"
    )
    cooperative_id_number: str | None = FieldStrOrNone("Cooperative ID Number")
    sbvp_sanborn_map_identifier: SbvpSanbornMapIdentifier | None = Field(
        alias="SBVP (SANBORN MAP IDENTIFIER)"  # todo - or None
    )
    dcp_commercial_study_area: str | None = FieldStrOrNone("DCP Commercial Study Area")
    tax_map_number_section_volume: str | None = FieldStrOrNone(
        "Tax Map Number Section & Volume"
    )
    reserved_for_tax_map_page_number: str | None = FieldStrOrNone(
        "Reserved for Tax Map Page Number"
    )
    x_y_coordinates_of_lot_centroid: str | None = FieldStrOrNone(
        "X-Y Coordinates of Lot Centroid"
    )
    business_improvement_district_bid: str | None = FieldStrOrNone(
        "Business Improvement District (BID)"
    )
    tpad_bin_status: str | None = FieldStrOrNone("TPAD BIN Status")
    tpad_new_bin: str | None = FieldStrOrNone("TPAD New BIN")
    tpad_new_bin_status: str | None = FieldStrOrNone("TPAD New BIN Status")
    tpad_conflict_flag: str | None = FieldStrOrNone("TPAD Conflict Flag")
    dcp_zoning_map: str | None = FieldStrOrNone("DCP Zoning Map")
    internal_use: str | None = FieldStrOrNone("Internal Use")
    number_of_entries_in_list_of_geographic_identifiers: str | None = FieldStrOrNone(
        "Number of Entries in List of Geographic Identifiers"
    )
    list_of_geographic_identifiers: list[ListOfGeographicIdentifiers] = Field(
        alias="LIST OF GEOGRAPHIC IDENTIFIERS"
    )
