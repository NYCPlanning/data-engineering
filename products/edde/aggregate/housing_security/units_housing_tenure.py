from aggregate.load_aggregated import ACSAggregator

aggregator = ACSAggregator(
    name="units_housing_tenure",
    dcp_base_variables=[
        "units_occupied_owner",
        "units_occupied_renter",
        "units_occupied",
    ],
    dcp_base_variables_conf={"include_race": True, "include_year_in_out_col": True},
    internal_review_filename="units_housing_tenure.csv",
    internal_review_category="housing_security",
)


units_housing_tenure = aggregator.run
