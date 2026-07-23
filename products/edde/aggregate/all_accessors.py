# Decennial Census imports
from aggregate.decennial_census import decennial_census_001020

# Housing production imports
from aggregate.housing_production import (
    affordable_housing,
    change_in_units,
    fraction_historic,
)

# Housing Security imports
# Three or more maintenance deficiences also left out as I'm not sure where we are getting data moving forward
# from aggregate.housing_security.three_or_more_maintenance_deficiencies import (
#     count_units_three_or_more_deficiencies,
# )
from aggregate.housing_security import (
    count_residential_evictions,
    dhs_shelter,
    homevalue_median,
    households_rent_burden,
    housing_lottery_applications,
    housing_lottery_leases,
    income_restricted_units,
    income_restricted_units_hpd,
    nycha_tenants,
    pums_2000_hsq_housing_tenure,
    rent_median,
    rent_stabilized_units,
    three_maintenance_units,
    units_affordable,
    units_housing_tenure,
    units_overcrowd,
)

# from aggregate.housing_security.eviction_cases_housing_court import eviction_cases
# PUMS imports
from aggregate.PUMS import (
    acs_pums_demographics,
    acs_pums_economics,
    pums_2000_demographics,
    pums_2000_economics,
)

# Quality of life imports
from aggregate.quality_of_life.traffic_fatalities import traffic_fatalities_injuries
from aggregate.quality_of_life.health_mortality import (
    infant_mortality,
    overdose_mortality,
    premature_mortality,
)
from aggregate.quality_of_life import (
    access_subway_and_access_ADA,
    access_to_broadband,
    access_to_jobs,
    access_to_openspace,
    access_transit_car,
    assault_hospitalizations,
    pedestrian_hospitalizations,
    get_education_outcome,
    health_diabetes,
    health_self_reported,
)

housing_production_accessors = [fraction_historic, change_in_units, affordable_housing]


QOL_accessors = [
    access_to_jobs,
    access_to_openspace,
    access_to_broadband,
    access_transit_car,
    access_subway_and_access_ADA,
    get_education_outcome,
    # covid_death,
    health_self_reported,
    infant_mortality,
    overdose_mortality,
    premature_mortality,
    health_diabetes,
    traffic_fatalities_injuries,
    assault_hospitalizations,
    pedestrian_hospitalizations,
]

housing_security_accessors = [
    dhs_shelter,
    count_residential_evictions,
    # eviction_cases,
    units_affordable,
    income_restricted_units,
    income_restricted_units_hpd,
    rent_stabilized_units,
    three_maintenance_units,
    pums_2000_hsq_housing_tenure,
    units_housing_tenure,
    homevalue_median,
    households_rent_burden,
    rent_median,
    units_overcrowd,
    nycha_tenants,
    housing_lottery_applications,
    housing_lottery_leases,
]

demographics_accessors = [
    decennial_census_001020,
    pums_2000_demographics,
    acs_pums_demographics,
]

economics_accessors = [pums_2000_economics, acs_pums_economics]


accessors = (
    housing_security_accessors
    + QOL_accessors
    + housing_production_accessors
    + demographics_accessors
    + economics_accessors
)


class Accessors:
    """Used for testing, exporting QOL/housing security/housing prod"""

    demographics = demographics_accessors
    economics = economics_accessors
    quality_of_life = QOL_accessors
    housing_production = housing_production_accessors
    housing_security = housing_security_accessors
    all = accessors
