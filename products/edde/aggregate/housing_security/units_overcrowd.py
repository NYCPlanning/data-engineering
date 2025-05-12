from aggregate.load_aggregated import ACSAggregator

units_overcrowd_aggregator = ACSAggregator(
    name="units_overcrowd",
    dcp_base_variables=["units_overcrowded", "units_notovercrowded"],
    dcp_base_variables_conf={
        "include_race": True,
        "include_year_in_out_col": True,
    },
    internal_review_filename="units_overcrowd.csv",
    internal_review_category="housing_security",
)

units_overcrowd = units_overcrowd_aggregator.run
