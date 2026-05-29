from aggregate.housing_security.DHS_shelter import dhs_shelter
from aggregate.housing_security.evictions_by_city_marshals import (
    count_residential_evictions,
)
from aggregate.housing_security.homevalue_median import homevalue_median
from aggregate.housing_security.households_rent_burden import households_rent_burden
from aggregate.housing_security.housing_lottery import (
    housing_lottery_applications,
    housing_lottery_leases,
)
from aggregate.housing_security.income_restricted_units import (
    income_restricted_units,
    income_restricted_units_hpd,
)
from aggregate.housing_security.nycha_tenants import nycha_tenants
from aggregate.housing_security.pums_2000_hsq_housing_tenure import (
    pums_2000_hsq_housing_tenure,
)
from aggregate.housing_security.rent_median import rent_median
from aggregate.housing_security.rent_stable_three_maintenance import (
    rent_stabilized_units,
    three_maintenance_units,
)
from aggregate.housing_security.units_affordable import units_affordable
from aggregate.housing_security.units_housing_tenure import units_housing_tenure
from aggregate.housing_security.units_overcrowd import units_overcrowd

all_accessors = [
    dhs_shelter,
    count_residential_evictions,
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
]

__all__ = [
    "dhs_shelter",
    "count_residential_evictions",
    "homevalue_median",
    "households_rent_burden",
    "housing_lottery_applications",
    "housing_lottery_leases",
    "income_restricted_units",
    "income_restricted_units_hpd",
    "nycha_tenants",
    "pums_2000_hsq_housing_tenure",
    "rent_median",
    "rent_stabilized_units",
    "three_maintenance_units",
    "units_affordable",
    "units_housing_tenure",
    "units_overcrowd",
    "all_accessors",
]
