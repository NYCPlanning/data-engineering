from aggregate.load_aggregated import ACSAggregator

aggregator = ACSAggregator(
    name="households_rent_burden",
    dcp_base_variables=["households_rb", "households_erb", "households_grapi"],
    dcp_base_variables_conf={"include_race": True, "include_year_in_out_col": True},
    internal_review_filename="households_rent_burden.csv",
    internal_review_category="households_rent_burden",
)

households_rent_burden = aggregator.run
