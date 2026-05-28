from aggregate.PUMS.pums_2000_demographics import pums_2000_demographics
from aggregate.PUMS.pums_2000_economics import pums_2000_economics
from aggregate.PUMS.pums_demographics import acs_pums_demographics
from aggregate.PUMS.pums_economics import acs_pums_economics

all_accessors = [
    pums_2000_demographics,
    pums_2000_economics,
    acs_pums_demographics,
    acs_pums_economics,
]

__all__ = [
    "pums_2000_demographics",
    "pums_2000_economics",
    "acs_pums_demographics",
    "acs_pums_economics",
    "all_accessors",
]
