from pydantic import BaseModel, Field


class BBL(BaseModel):
    bbl: str = Field(alias="BOROUGH BLOCK LOT (BBL)")
    borough_code: str = Field(alias="Borough Code")
    tax_block: str = Field(alias="Tax Block")
    tax_lot: str = Field(alias="Tax Lot")


class UnitSortFormat(BaseModel):
    unit_sort_format: str = Field(alias="UNIT - SORT FORMAT")
    unit_type: str = Field(alias="Unit - Type")
    unit_identifier: str = Field(alias="Unit - Identifier")


class LowBblOfThisBuildingSCondominiumUnits(BaseModel):
    low_bbl_of_this_buildings_condominium_units: str = Field(
        alias="LOW BBL OF THIS BUILDING'S CONDOMINIUM UNITS"
    )
    borough_code: str = Field(alias="Borough Code")
    tax_block: str = Field(alias="Tax Block")
    tax_lot: str = Field(alias="Tax Lot")


class HighBblOfThisBuildingSCondominiumUnits(BaseModel):
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


class Result1A(BaseModel):
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
    bbl: BBL = Field(alias="BOROUGH BLOCK LOT (BBL)")
    filler_for_tax_lot_version_number: str = Field(
        alias="Filler for Tax Lot Version Number"
    )
    low_house_number_display_format: str = Field(
        alias="Low House Number - Display Format"
    )
    low_house_number_sort_format: str = Field(alias="Low House Number - Sort Format")
    bin: str = Field(alias="Building Identification Number (BIN)")
    street_attribute_indicators: str = Field(alias="Street Attribute Indicators")
    reason_code_2: str = Field(alias="Reason Code 2")
    reason_code_qualifier_2: str = Field(alias="Reason Code Qualifier 2")
    warning_code_2: str = Field(alias="Warning Code 2")
    geosupport_return_code_2: str = Field(alias="Geosupport Return Code 2 (GRC 2)")
    message_2: str = Field(alias="Message 2")
    node_number: str = Field(alias="Node Number")
    unit_sort_format: UnitSortFormat = Field(alias="UNIT - SORT FORMAT")
    unit_display_format: str = Field(alias="Unit - Display Format")
    nin: str = Field(alias="NIN")
    street_attribute_indicator: str = Field(alias="Street Attribute Indicator")
    reason_code: str = Field(alias="Reason Code")
    reason_code_qualifier: str = Field(alias="Reason Code Qualifier")
    warning_code: str = Field(alias="Warning Code")
    geosupport_return_code: str = Field(alias="Geosupport Return Code (GRC)")
    message: str = Field(alias="Message")
    number_of_street_codes_and_street_names_in_list: str = Field(
        alias="Number of Street Codes and Street Names in List"
    )
    list_of_street_codes: list = Field(alias="List of Street Codes")
    list_of_street_names: list = Field(alias="List of Street Names")
    continuous_parity_indicator_duplicate_address_indicator: str = Field(
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
    low_bbl_of_this_buildings_condominium_units: LowBblOfThisBuildingSCondominiumUnits = Field(
        alias="LOW BBL OF THIS BUILDING'S CONDOMINIUM UNITS"
    )
    filler_for_tax_lot_version_no_of_low_bbl: str = Field(
        alias="Filler for Tax Lot Version No. of Low BBL"
    )
    high_bbl_of_this_buildings_condominium_units: HighBblOfThisBuildingSCondominiumUnits = Field(
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
    list_of_4_lg_cs: str = Field(alias="List of 4 LGCs")
    number_of_entries_in_list_of_geographic_identifiers: str = Field(
        alias="Number of Entries in List of Geographic Identifiers"
    )
    list_of_geographic_identifiers: list[ListOfGeographicIdentifiers] = Field(
        alias="LIST OF GEOGRAPHIC IDENTIFIERS"
    )
