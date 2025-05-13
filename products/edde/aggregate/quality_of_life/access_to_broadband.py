from aggregate.load_aggregated import ACSAggregator

access_to_broadband_agg = ACSAggregator(
    name="access_to_broadband",
    dcp_base_variables=["access_households", "access_computer", "access_broadband"],
    dcp_base_variables_conf={
        "include_race": True,
        "include_year_in_out_col": False,
    },
    internal_review_filename="access_to_broadband.csv",
    internal_review_category="quality_of_life",
    include_start_year=False,
)

access_to_broadband = access_to_broadband_agg.run
