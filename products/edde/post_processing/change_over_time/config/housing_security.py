"""
Configuration for housing_security change calculations.
Labels copied exactly from 2025-convert-housing_security.js line 9.
"""

# Labels define which base variables to process
# Format: {variable}_change_{type}
LABELS = [
    "dhs_shelter_change_count",
    "units_occupied_owner_change_count",
    "units_occupied_renter_change_count",
    "units_occupied_change_count",
    "units_occupied_owner_change_anh_count",
    "units_occupied_owner_change_bnh_count",
    "units_occupied_owner_change_hsp_count",
    "units_occupied_owner_change_wnh_count",
    "units_occupied_renter_change_anh_count",
    "units_occupied_renter_change_bnh_count",
    "units_occupied_renter_change_hsp_count",
    "units_occupied_renter_change_wnh_count",
    "units_occupied_change_anh_count",
    "units_occupied_change_bnh_count",
    "units_occupied_change_hsp_count",
    "units_occupied_change_wnh_count",
    "homevalue_median_change_median",
    "homevalue_median_change_anh_median",
    "homevalue_median_change_bnh_median",
    "homevalue_median_change_hsp_median",
    "homevalue_median_change_wnh_median",
    "households_rb_change_count",
    "households_erb_change_count",
    "households_grapi_change_count",
    "households_rb_change_anh_count",
    "households_rb_change_bnh_count",
    "households_rb_change_hsp_count",
    "households_rb_change_wnh_count",
    "households_erb_change_anh_count",
    "households_erb_change_bnh_count",
    "households_erb_change_hsp_count",
    "households_erb_change_wnh_count",
    "households_grapi_change_anh_count",
    "households_grapi_change_bnh_count",
    "households_grapi_change_hsp_count",
    "households_grapi_change_wnh_count",
    "rent_median_change_median",
    "rent_median_change_anh_median",
    "rent_median_change_bnh_median",
    "rent_median_change_hsp_median",
    "rent_median_change_wnh_median",
    "units_payingrent_change_count",
    "units_payingrent_change_anh_count",
    "units_payingrent_change_bnh_count",
    "units_payingrent_change_hsp_count",
    "units_payingrent_change_wnh_count",
    "units_overcrowded_change_count",
    "units_notovercrowded_change_count",
    "units_overcrowded_change_anh_count",
    "units_overcrowded_change_bnh_count",
    "units_overcrowded_change_hsp_count",
    "units_overcrowded_change_wnh_count",
    "units_notovercrowded_change_anh_count",
    "units_notovercrowded_change_bnh_count",
    "units_notovercrowded_change_hsp_count",
    "units_notovercrowded_change_wnh_count",
]

# Special yearband mappings (most use default 0812 -> 1923)
# dhs_shelter uses 2020 -> 2022
SPECIAL_YEARBANDS = {"dhs_shelter": ("2020", "2022")}
