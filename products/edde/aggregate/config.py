"""Centralized configuration from recipe for aggregate module."""

from pathlib import Path

from dcpy.lifecycle.builds import get_recipe_lock

# Load recipe configuration once
PRODUCT_PATH = Path(__file__).parent.parent
_recipe_lock = get_recipe_lock(PRODUCT_PATH)

# Core ACS year bands from recipe custom
acs_prev_year_band = _recipe_lock.custom.get("ACS_PREV_YEAR_BAND", "0812")
acs_current_year_band = _recipe_lock.custom.get("ACS_CURRENT_YEAR_BAND", "2024")

# Derived year suffixes (last 2 digits of year bands)
acs_prev_year_suffix = acs_prev_year_band[-2:]
acs_current_year_suffix = acs_current_year_band[-2:]

# Semantic year name to suffix mapping (includes both legacy "prev"/"current" and full yearbands)
acs_year_suffix_map = {
    "prev": acs_prev_year_suffix,
    "current": acs_current_year_suffix,
    acs_prev_year_band: acs_prev_year_suffix,  # e.g., "0812" -> "12"
    acs_current_year_band: acs_current_year_suffix,  # e.g., "2024" -> "24"
}

# Year suffix to full year band mapping
acs_years_end_to_full = {
    acs_prev_year_suffix: acs_prev_year_band,
    acs_current_year_suffix: acs_current_year_band,
}

# Health mortality configuration from recipe custom section
health_mortality_config = _recipe_lock.custom.get("health_mortality", {})
health_mortality_latest_year = health_mortality_config.get("latest_year", "1620")
health_mortality_puma_baseline_years = health_mortality_config.get(
    "puma_baseline_years", ["0004", "1014"]
)
health_mortality_baseline_years = health_mortality_config.get(
    "baseline_years", ["2000", "2010"]
)

# Traffic fatalities configuration from recipe custom section
traffic_fatalities_config = _recipe_lock.custom.get("traffic_fatalities", {})
traffic_fatalities_year_ranges = traffic_fatalities_config.get(
    "year_ranges", {"1014": [2010, 2015], "1620": [2016, 2021]}
)

# DHS shelter configuration from recipe custom section
dhs_shelter_config = _recipe_lock.custom.get("dhs_shelter", {})
dhs_shelter_years = dhs_shelter_config.get("years", ["2020", "2022"])
