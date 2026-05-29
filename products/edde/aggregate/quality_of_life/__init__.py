from aggregate.quality_of_life.access_subway_and_access_ADA import (
    access_subway_and_access_ADA,
)
from aggregate.quality_of_life.access_to_broadband import access_to_broadband
from aggregate.quality_of_life.access_to_jobs import access_to_jobs
from aggregate.quality_of_life.access_to_openspace import access_to_openspace
from aggregate.quality_of_life.access_transit_car import access_transit_car
from aggregate.quality_of_life.diabetes_self_report import (
    health_diabetes,
    health_self_reported,
)
from aggregate.quality_of_life.education_outcome import get_education_outcome
from aggregate.quality_of_life.safety_ped_aslt_hospitalizations import (
    assault_hospitalizations,
)

all_accessors = [
    access_subway_and_access_ADA,
    access_to_broadband,
    access_to_jobs,
    access_to_openspace,
    access_transit_car,
    health_diabetes,
    health_self_reported,
    get_education_outcome,
    assault_hospitalizations,
]

__all__ = [
    "access_subway_and_access_ADA",
    "access_to_broadband",
    "access_to_jobs",
    "access_to_openspace",
    "access_transit_car",
    "health_diabetes",
    "health_self_reported",
    "get_education_outcome",
    "assault_hospitalizations",
    "all_accessors",
]
