"""
Configuration for quality_of_life change calculations.
Labels copied exactly from convert-qol.js line 9.
"""

# Labels define which base variables to process
# Format: {variable}_change_{type}
LABELS = [
    "access_carcommute_change_count",
    "access_workers16pl_change_count",
    "access_carcommute_change_anh_count",
    "access_carcommute_change_bnh_count",
    "access_carcommute_change_hsp_count",
    "access_carcommute_change_wnh_count",
    "access_workers16pl_change_anh_count",
    "access_workers16pl_change_bnh_count",
    "access_workers16pl_change_hsp_count",
    "access_workers16pl_change_wnh_count",
]

# No special yearband mappings (all use default 0812 -> 1923)
SPECIAL_YEARBANDS = {}
