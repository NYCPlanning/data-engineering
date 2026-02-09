from abc import abstractmethod
from dataclasses import dataclass


@dataclass
class AdminGeography:
    geography_type: str
    cpdb_geography_type: str
    table_name: str
    geography_id: str
    geography_name: str


@dataclass
class AdminGeographies:
    @abstractmethod
    def generate_geography(self, geography_number: int) -> AdminGeography:
        raise NotImplementedError("AdminGeographies is an abstract class")


@dataclass
class CityCouncilDistricts(AdminGeographies):
    count: int
    geography_type: str = "City Council District"
    cpdb_geography_type: str = "council"

    def generate_geography(self, geography_number: int) -> AdminGeography:
        table_name = "city_council_district_" + str(geography_number)
        geography_id = str(geography_number)
        geography_name = "City Council District " + str(geography_number)

        return AdminGeography(
            self.geography_type,
            self.cpdb_geography_type,
            table_name,
            geography_id,
            geography_name,
        )


@dataclass
class BoroughCommunityDistricts(AdminGeographies):
    borough_name: str
    borough_code: int
    count: int
    boundary_type: str = "Community District"
    cpdb_geography_type: str = "commboard"

    def generate_geography(self, geography_number: int) -> AdminGeography:
        table_suffix = (
            self.borough_name.replace(" ", "_") + "_cd" + str(geography_number).zfill(2)
        ).lower()
        table_name = "community_district_" + table_suffix

        geography_id = str(self.borough_code) + str(geography_number).zfill(2)
        geography_name = f"{self.borough_name} CD{geography_number}"
        return AdminGeography(
            self.boundary_type,
            self.cpdb_geography_type,
            table_name,
            geography_id,
            geography_name,
        )


ALL_CITY_COUNCIL_DISTRICTS = CityCouncilDistricts(count=51)

ALL_COMMUNITY_DISTRICT_BOROUGHS = [
    BoroughCommunityDistricts(borough_name="Manhattan", borough_code=1, count=12),
    BoroughCommunityDistricts(borough_name="Bronx", borough_code=2, count=12),
    BoroughCommunityDistricts(borough_name="Brooklyn", borough_code=3, count=18),
    BoroughCommunityDistricts(borough_name="Queens", borough_code=4, count=14),
    BoroughCommunityDistricts(borough_name="Staten Island", borough_code=5, count=3),
]


def generate_city_council_districts(
    city_council_districts: CityCouncilDistricts,
) -> list[AdminGeography]:
    ccds = []
    for city_council_district_number in range(1, city_council_districts.count + 1):
        ccd = city_council_districts.generate_geography(city_council_district_number)
        ccds.append(ccd)
    return ccds


def generate_community_districts(
    community_district_boroughs: list[BoroughCommunityDistricts],
) -> list[AdminGeography]:
    cds = []
    for borough in community_district_boroughs:
        for community_district_number in range(1, borough.count + 1):
            cd = borough.generate_geography(community_district_number)
            cds.append(cd)
    return cds


def generate_all_admin_geographies() -> list[AdminGeography]:
    return generate_city_council_districts(
        ALL_CITY_COUNCIL_DISTRICTS
    ) + generate_community_districts(ALL_COMMUNITY_DISTRICT_BOROUGHS)
