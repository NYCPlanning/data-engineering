from aggregate.all_accessors import Accessors
from build import get_ignored_indicators

accessors = Accessors()


def get_by_geo():
    """housing security parameter is temporary"""
    ignored_indicators = get_ignored_indicators()
    accessors_list = [a for a in accessors.all if a.__name__ not in ignored_indicators]
    by_puma = []
    by_borough = []
    by_citywide = []

    for a in accessors_list:
        by_puma.append((a("puma"), a.__name__))
        by_borough.append((a("borough"), a.__name__))
        by_citywide.append((a("citywide"), a.__name__))
    return by_puma, by_borough, by_citywide
