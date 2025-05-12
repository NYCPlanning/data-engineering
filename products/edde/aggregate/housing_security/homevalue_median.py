from aggregate.load_aggregated import ACSAggregator

aggregator = ACSAggregator(
    name="homevalue_median",
    dcp_base_variables=[
        "homevalue_median",
    ],
    dcp_base_variables_conf={"include_race": True, "include_year_in_out_col": True},
    internal_review_filename="homevalue_median.csv",
    internal_review_category="housing_security",
)

homevalue_median = aggregator.run
