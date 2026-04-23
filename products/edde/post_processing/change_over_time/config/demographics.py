"""
Configuration for demographics change calculations.
Labels copied exactly from convert-demo.js line 14.
"""

# Labels define which base variables to process
# Format: {variable}_change_{type}
LABELS = [
    "age_median_change_anh_median",
    "age_median_change_bnh_median",
    "age_median_change_hsp_median",
    "age_median_change_median",
    "age_median_change_wnh_median",
    "age_p16t64_change_anh_count",
    "age_p16t64_change_bnh_count",
    "age_p16t64_change_count",
    "age_p16t64_change_hsp_count",
    "age_p16t64_change_wnh_count",
    "age_p5pl_change_anh_count",
    "age_p5pl_change_bnh_count",
    "age_p5pl_change_count",
    "age_p5pl_change_hsp_count",
    "age_p5pl_change_wnh_count",
    "age_p65pl_change_anh_count",
    "age_p65pl_change_bnh_count",
    "age_p65pl_change_count",
    "age_p65pl_change_hsp_count",
    "age_p65pl_change_wnh_count",
    "age_popu16_change_anh_count",
    "age_popu16_change_bnh_count",
    "age_popu16_change_count",
    "age_popu16_change_hsp_count",
    "age_popu16_change_wnh_count",
    "fb_change_anh_count",
    "fb_change_bnh_count",
    "fb_change_count",
    "fb_change_hsp_count",
    "fb_change_wnh_count",
    "lep_change_anh_count",
    "lep_change_bnh_count",
    "lep_change_count",
    "lep_change_hsp_count",
    "lep_change_wnh_count",
    "pop_change_anh_count",
    "pop_change_bnh_count",
    "pop_change_count",
    "pop_change_hsp_count",
    "pop_change_onh_count",
    "pop_change_wnh_count",
    "pop_denom_change_anh_count",
    "pop_denom_change_bnh_count",
    "pop_denom_change_count",
    "pop_denom_change_hsp_count",
    "pop_denom_change_wnh_count",
]

# No special yearband mappings (all use default 0812 -> 1923)
SPECIAL_YEARBANDS = {}

# Special formatting: age_median variables use 1 decimal place for change value
# (see convert-demo.js lines 76-80)
AGE_MEDIAN_LABELS = [
    "age_median_change_anh_median",
    "age_median_change_bnh_median",
    "age_median_change_hsp_median",
    "age_median_change_median",
    "age_median_change_wnh_median",
]
