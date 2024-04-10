HVS_borough_mapper = {
    1: "Bronx",
    2: "Brooklyn",
    3: "Manhattan",
    4: "Queens",
    5: "Staten Island",
}


def HVS_borough_clean(raw_borough):
    return HVS_borough_mapper[int(raw_borough)]


def HVS_PUMA_constructor(borough, sub_borough_area):
    """Needs to be written by looking at maps from here https://www.census.gov/geographies/reference-maps/2017/demo/nychvs/sub-bourough-maps.html
    and comparing to https://www1.nyc.gov/assets/planning/download/pdf/data-maps/nyc-population/census2010/puma_cd_map.pdf"""
    return None
