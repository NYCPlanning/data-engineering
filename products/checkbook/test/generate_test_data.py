import geopandas as gpd
import pandas as pd
from shapely import Point


def generate_cpdb_test_data() -> list[gpd.GeoDataFrame]:
    # most recent cpdb
    cpdb_data_1 = gpd.GeoDataFrame(
        {
            "maprojid": ["ABCDEFG", "ZZZZZZ", "TESTING"],
            "typecatego": ["Fixed Asset", "Lump Sum", "None"],
            "geometry": [Point(0, 0), Point(0, 12), Point(1, 1)],
        }
    )
    cpdb_data_2 = gpd.GeoDataFrame(
        {
            "maprojid": ["ABCDEFG", "ZZZZZZ"],
            "typecatego": ["Fixed Asset", "ITT, Vehicles and Equipment"],
            "geometry": [Point(0, 0), Point(1, 1)],
        }
    )
    # oldest cpdb
    cpdb_data_3 = gpd.GeoDataFrame(
        {
            "maprojid": ["ABCDEFG", "ZZZZZZ"],
            "typecatego": ["Fixed Asset", "Lump Sum"],
            "geometry": [Point(5, 8), Point(13, 1)],
        }
    )
    cpdb_data = [cpdb_data_1, cpdb_data_2, cpdb_data_3]
    return cpdb_data


def generate_checkbook_test_data() -> pd.DataFrame:
    checkbook_data = pd.DataFrame(
        {
            "capital_project": [
                "GFEDCBA 100",
                "ZZZZZZ 100",
                "ABCDEFG 123",
                "ABCDEFG 100",
                "ZZZZZZ 100",
                "ABCDEFG 100",
            ],
            "Contract Purpose": [
                "Auditorium 1",
                "Vehicle 2",
                "Lump Sum 3",
                "Park 4",
                "Park 5",
                "Vehicle 6",
            ],
            "Agency": [
                "Agency 1",
                "Agency 2",
                "Agency 3",
                "Agency 4",
                "Agency 5",
                "Agency 6",
            ],
            "Budget Code": [
                "Auditorium 1",
                "Vehicle 2",
                "Lump Sum 3",
                "Park 4",
                "Park 5",
                "Vehicle 6",
            ],
            "Check Amount": [1000, 2000, 3000, 99999999, -1000, 5000],
        }
    )
    return checkbook_data


def generate_expected_grouped_checkbook() -> pd.DataFrame:
    checkbook_data = pd.DataFrame(
        {
            "fms_id": ["GFEDCBA", "ZZZZZZ", "ABCDEFG"],
            "check_amount": [1000, 2000, 8000],
            "contract_purpose": ["Auditorium 1", "Vehicle 2", "Lump Sum 3;Vehicle 6"],
            "budget_code": ["Auditorium 1", "Vehicle 2", "Lump Sum 3;Vehicle 6"],
            "agency": ["Agency 1", "Agency 2", "Agency 3;Agency 6"],
        }
    )
    return checkbook_data


def generate_expected_cpdb_join() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "maprojid": ["ABCDEFG", "ZZZZZZ", "TESTING"],
            "typecatego": ["Fixed Asset", "Lump Sum", "None"],
            "geometry": [Point(0, 0), Point(0, 12), Point(1, 1)],
        }
    )


def generate_expected_final_data() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "fms_id": ["GFEDCBA", "ZZZZZZ", "ABCDEFG"],
            "contract_purpose": ["Auditorium 1", "Vehicle 2", "Lump Sum 3;Vehicle 6"],
            "agency": ["Agency 1", "Agency 2", "Agency 3;Agency 6"],
            "budget_code": ["Auditorium 1", "Vehicle 2", "Lump Sum 3;Vehicle 6"],
            "check_amount": [1000, 2000, 8000],
            "bc_category": [
                "Fixed Asset",
                "ITT, Vehicles, and Equipment",
                "ITT, Vehicles, and Equipment",
            ],
            "cp_category": [
                "Fixed Asset",
                "ITT, Vehicles, and Equipment",
                "ITT, Vehicles, and Equipment",
            ],
            "maprojid": [None, "ZZZZZZ", "ABCDEFG"],
            "cpdb_category": ["None", "Lump Sum", "Fixed Asset"],
            "geometry": [None, Point(0, 12), Point(0, 0)],
            "has_geometry": [False, True, True],
            "final_category": [
                "Fixed Asset",
                "ITT, Vehicles, and Equipment",
                "Fixed Asset",
            ],
        }
    )
