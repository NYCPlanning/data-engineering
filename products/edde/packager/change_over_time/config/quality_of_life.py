"""
Configuration for quality_of_life change calculations.
Labels copied exactly from convert-qol.js line 9.
"""

import sys
from pathlib import Path

# Add parent directories to path to import from aggregate
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from aggregate.config import traffic_fatalities_year_ranges

# Get traffic year bands from recipe config
# traffic_fatalities_year_ranges is a dict like {"1014": [2010, 2015], "1620": [2016, 2021]}
# Extract the keys (year band codes) and sort them to get old -> new
traffic_year_bands = sorted(traffic_fatalities_year_ranges.keys())
if len(traffic_year_bands) >= 2:
    traffic_old_year = traffic_year_bands[0]  # e.g., "1014"
    traffic_new_year = traffic_year_bands[-1]  # e.g., "1620"
else:
    # Fallback to hardcoded values if config is missing
    traffic_old_year = "1014"
    traffic_new_year = "1620"

# Labels define which base variables to process
# Format: {variable}_change_{type}
LABELS = [
    # Commute (ACS years: 0812 -> 2024)
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
    # Traffic injuries (Vision Zero years from recipe config)
    "safety_trafficinjuries_change_rate",
    "safety_trafficinjuries_ped_change_rate",
    "safety_trafficinjuries_cycle_change_rate",
    "safety_trafficinjuries_motorist_change_rate",
    # Traffic fatalities (Vision Zero years from recipe config)
    "safety_trafficfatalities_change_rate",
]

# Special yearband mappings for indicators that don't use the default 0812 -> 2024
# Traffic injuries and fatalities use Vision Zero View years from recipe config
SPECIAL_YEARBANDS = {
    "safety_trafficinjuries": (traffic_old_year, traffic_new_year),
    "safety_trafficinjuries_ped": (traffic_old_year, traffic_new_year),
    "safety_trafficinjuries_cycle": (traffic_old_year, traffic_new_year),
    "safety_trafficinjuries_motorist": (traffic_old_year, traffic_new_year),
    "safety_trafficfatalities": (traffic_old_year, traffic_new_year),
}
