from typing import List

import pandas as pd
from ingest.PUMS.PUMS_cleaner import PUMSCleaner
from ingest.PUMS.PUMS_query_manager import get_urls
from ingest.PUMS.PUMS_request import make_GET_request
from ingest.PUMS.variable_generator import variables_for_processing
from utils.geo_helpers import clean_PUMAs, puma_to_borough

"""To do: make this central module from which all other code is called. Write
class method for aggregate step to access.  Class method will return cached data or
initalize a PUMSData object and use it to save a .pkl"""


def make_PUMS_cache_fn(
    year: int, variable_types=None, limited_PUMA=False, include_rw=False
):
    fn = f"PUMS_{'_'.join(variable_types)}"
    fn = f"{fn}_{year}"
    if limited_PUMA:
        fn += "_limitedPUMA"
    if not include_rw:
        fn += "_noRepWeights"
    return f"data/{fn}.pkl"


class PUMSData:
    """This class encapsulates url used to fetch PUMS data, variables the data includes,
    data itself, and the code to clean it. Currently somewhat deprecated"""

    def __init__(
        self,
        variable_types: List = ["demographics"],
        limited_PUMA: bool = False,
        year: int = 2019,
        include_rw: bool = True,
        household: bool = False,
    ):
        """Pulling PUMS data with replicate weights requires multiple GET
        requests as there is 50 variable max for each GET request. This class is
        responsible for merging these three GET requests into one dataframe

        vi refers to variables of interest. These are the variables the
        equitable development tool with use. Contrast to rw

        rw refers to replicate weights. Merged to variables of interest

        GET variables refer to variables queried from the API. These include variables
        of interest and replicate weights

        urls is dictionary that maps a set of GET variables to two GET request URLs,
        one for each geographic region

        :variables: this class needs variables attrs to know which columns to clean
        :urls: tuple of two urls, one with each geographic regions
        :data: dataframe originally populated with variables
        """
        self.include_rw = include_rw
        if not include_rw:
            self.rw_cols = []
        elif household:
            self.rw_cols = [f"WGTP{x}" for x in range(1, 81)]
        else:
            self.rw_cols = [f"PWGTP{x}" for x in range(1, 81)]
        self.cache_path = self.get_cache_fn(
            variable_types, limited_PUMA, year, include_rw
        )
        self.variables = variables_for_processing(variable_types=variable_types)
        self.limited_PUMA = limited_PUMA
        self.year = year
        self.household = household
        urls = get_urls(
            variable_types=variable_types,
            year=year,
            limited_PUMA=limited_PUMA,
            include_rw=self.include_rw,
            household=self.household,
        )
        self.urls = urls
        self.vi_data: pd.DataFrame = None
        self.vi_data_raw: pd.DataFrame = None
        self.rw_one_data: pd.DataFrame = None
        self.rw_two_data: pd.DataFrame = None
        self.download_and_cache()

    @classmethod
    def get_cache_fn(self, variable_types, limited_PUMA, year, include_rw):
        return make_PUMS_cache_fn(
            variable_types=variable_types,
            limited_PUMA=limited_PUMA,
            year=year,
            include_rw=include_rw,
        )

    def populate_dataframes(self):
        if self.year == 2019:
            for k, i in self.urls.items():
                data_region_one = make_GET_request(
                    i[0], f"get request for {k} region one"
                )
                data_region_two = make_GET_request(
                    i[1], f"get request for {k} region two"
                )
                data = data_region_one.append(data_region_two)
                data = rename_PUMA_col(data, "PUMA")
                if self.household:
                    data = data.loc[data["SPORDER"] == "1"]
                self.assign_data_to_attr(k, data)
        if self.year == 2012:
            for k, i in self.urls.items():
                r1_2000 = make_GET_request(i[0], f"get req for {k} region 1 2000 PUMAs")
                r1_2000 = rename_PUMA_col(r1_2000, "PUMA00")

                r1_2010 = make_GET_request(i[1], f"get req for {k} region 1 2010 PUMAs")
                r1_2010 = rename_PUMA_col(r1_2010, "PUMA10")

                r2_2000 = make_GET_request(i[2], f"get req for {k} region 2 2000 PUMAs")
                r2_2000 = rename_PUMA_col(r2_2000, "PUMA00")

                r2_2010 = make_GET_request(i[3], f"get req for {k} region 2 2010 PUMAs")
                r2_2010 = rename_PUMA_col(r2_2010, "PUMA10")
                data = r1_2000.append(r1_2010)
                data = data.append(r2_2000)
                data = data.append(r2_2010)
                self.assign_data_to_attr(k, data)

        self.vi_data_raw = self.vi_data.copy(deep=True)

    def assign_data_to_attr(self, k, data):
        attr_name = f"{k}_data"
        self.__setattr__(attr_name, data)
        self.assign_identifier(attr_name)

    def download_and_cache(self):
        self.populate_dataframes()
        if self.include_rw:
            self.merge_rw()
            self.merge_vi_rw()

        self.clean_data()
        self.cache()

    def cache(self):
        self.vi_data.to_pickle(self.cache_path)

    def assign_identifier(self, attr_name):
        df = self.__getattribute__(attr_name)  #
        df["person_id"] = df["SERIALNO"] + df["SPORDER"]
        df.set_index("person_id", inplace=True)
        df.drop(columns=["SPORDER"], inplace=True)

    def clean_data(self):
        self.vi_data["puma"] = self.vi_data["puma"].apply(clean_PUMAs)
        self.vi_data["borough"] = self.vi_data.apply(axis=1, func=puma_to_borough)
        self.vi_data["citywide"] = "citywide"

        if self.household:
            self.vi_data["WGTP"] = self.vi_data["WGTP"].astype(int)
            self.vi_data[self.rw_cols] = self.vi_data[self.rw_cols].astype(int)
        else:
            self.vi_data["PWGTP"] = self.vi_data["PWGTP"].astype(int)
            self.vi_data[self.rw_cols] = self.vi_data[self.rw_cols].astype(int)
        cleaner = PUMSCleaner()
        for v in self.variables:
            self.vi_data = cleaner.__getattribute__(v[1])(self.vi_data, v[0])

    def merge_rw(self):
        """Merge two dataframes of replicate weights into one"""
        cols_to_drop = [
            "ST",
            "puma",
            "SERIALNO",
        ]  # add serial number to drop before the merge
        self.rw_one_data.drop(columns=cols_to_drop, inplace=True)
        self.rw_two_data.drop(columns=cols_to_drop, inplace=True)
        self.rw = self.rw_one_data.merge(
            self.rw_two_data, left_index=True, right_index=True
        )

    def merge_vi_rw(self):
        """Add replicate weights to the dataframe with variables of interest"""
        self.vi_data = self.vi_data.merge(self.rw, left_index=True, right_index=True)


def rename_PUMA_col(df, PUMA_col_name):
    df.rename(columns={PUMA_col_name: "puma"}, inplace=True)
    return df
