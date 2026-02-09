import numpy as np
from build_scripts.build import (
    _assign_checkbook_category,
    _assign_final_category,
    _clean_checkbook,
    _clean_joined_checkbook_cpdb,
    _group_checkbook,
    _join_checkbook_geoms,
    _merge_cpdb_geoms,
)

from test.generate_test_data import (
    generate_checkbook_test_data,
    generate_cpdb_test_data,
    generate_expected_cpdb_join,
    generate_expected_final_data,
    generate_expected_grouped_checkbook,
)

CPDB_GDF_LIST = generate_cpdb_test_data()
CHECKBOOK_TEST = generate_checkbook_test_data()


class TestCheckbook:
    """
    tests that validate Checkbook NYC input data,
    cleaning and groupby transformations before joining
    to CPDB geoms
    """

    cleaned_checkbook_df = _clean_checkbook(CHECKBOOK_TEST)
    grouped_checkbook_df = _group_checkbook(cleaned_checkbook_df)
    expected_grouped_checkbook_df = generate_expected_grouped_checkbook()

    def test_columns(self):
        # checks that grouped checkbook contains the expected columns
        assert (self.grouped_checkbook_df.columns).equals(
            self.expected_grouped_checkbook_df.columns
        )

    def test_rows(self):
        # checks that grouped checkbook contains the expected number of rows
        assert (
            self.grouped_checkbook_df.shape[0]
            == self.expected_grouped_checkbook_df.shape[0]
        )

    def test_check_nonneg(self):
        # checks that no checks in the cleaned checkbook df are negative
        assert (self.cleaned_checkbook_df["check_amount"] >= 0).all()

    def test_fms_id_exists(self):
        # checks that `fms_id` is a column in the dataset
        assert "fms_id" in self.grouped_checkbook_df.columns

    def test_null_fms_id(self):
        # checks that no row in grouped checkbook has a null value for `fms_id`
        assert np.where(self.grouped_checkbook_df["fms_id"].isnull())

    def test_unique_fms_id(self):
        # checks that all fms_ids in test grouped checkbook are unique
        assert not self.grouped_checkbook_df["fms_id"].duplicated().any()

    def test_groupby(self):
        # checks that the results of running checkbook cleaning and grouping on test data match the expected output
        expected_result = self.expected_grouped_checkbook_df.set_index(
            "fms_id"
        ).sort_index()
        result = self.grouped_checkbook_df.set_index("fms_id").sort_index()
        assert result.equals(expected_result)


class TestCPDB:
    """
    tests that validate CPDB geoms and transformations
    before joining to Checkbook NYC capital projects
    """

    cpdb_df = _merge_cpdb_geoms(CPDB_GDF_LIST)
    expected_cpdb_df = generate_expected_cpdb_join()

    def test_columns(self):
        assert self.cpdb_df.columns.equals(self.expected_cpdb_df.columns)

    def test_rows(self):
        assert self.cpdb_df.shape[0] == self.expected_cpdb_df.shape[0]

    def test_unique_maprojid(self):
        # checks that there are no duplicate capital projects in merged cpdb
        assert not self.cpdb_df["maprojid"].duplicated().any()

    def test_null_geometry(self):
        # checks that there are no null geometries in the merged cpdb test data
        assert np.where(self.cpdb_df["geometry"].isnull())

    def test_valid_geometry(self):
        """
        tests that all geometries in merged cpdb are valid;
        will be more useful as we look toward increasing the
        sources of geometries for historical liquidations
        beyond CPDB
        """
        assert self.cpdb_df["geometry"].is_valid.all()

    def test_matches_expected(self):
        # checks that result of merging test cpdb data matches expected result
        assert self.cpdb_df.equals(self.expected_cpdb_df)


class TestHistoricalLiquidations:
    """
    tests that validate the build output, i.e. the final
    Historical Liquidations dataset
    """

    cpdb = _merge_cpdb_geoms(CPDB_GDF_LIST)
    checkbook = _group_checkbook(_clean_checkbook(CHECKBOOK_TEST))
    cat_checkbook = _assign_checkbook_category(checkbook)
    join = _join_checkbook_geoms(cat_checkbook, cpdb)
    clean_join = _clean_joined_checkbook_cpdb(join)[
        [
            "fms_id",
            "contract_purpose",
            "agency",
            "budget_code",
            "check_amount",
            "bc_category",
            "cp_category",
            "maprojid",
            "cpdb_category",
            "geometry",
            "has_geometry",
        ]
    ]
    historical_liquidations = (
        _assign_final_category(clean_join).set_index("fms_id").sort_index()
    )  # align expected and actual results rowwise
    expected_historical_liquidations = (
        generate_expected_final_data().set_index("fms_id").sort_index()
    )  # align expected and actual results rowwise

    def test_columns(self):
        assert self.historical_liquidations.columns.equals(
            self.expected_historical_liquidations.columns
        )

    def test_rows(self):
        assert (
            self.historical_liquidations.shape[0]
            == self.expected_historical_liquidations.shape[0]
        )

    def test_cp_category(self):
        assert self.historical_liquidations["cp_category"].equals(
            self.expected_historical_liquidations["cp_category"]
        )

    def test_bc_category(self):
        assert self.historical_liquidations["bc_category"].equals(
            self.expected_historical_liquidations["bc_category"]
        )

    def test_cpdb_category(self):
        assert self.historical_liquidations["cpdb_category"].equals(
            self.expected_historical_liquidations["cpdb_category"]
        )

    def test_final_category(self):
        assert self.historical_liquidations["final_category"].equals(
            self.expected_historical_liquidations["final_category"]
        )

    def test_all_categories(self):
        cols = ["cpdb_category", "bc_category", "cp_category", "final_category"]
        assert self.historical_liquidations[cols].equals(
            self.expected_historical_liquidations[cols]
        )

    def test_matches_expected(self):
        assert self.historical_liquidations.equals(
            self.expected_historical_liquidations
        )
