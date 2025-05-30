from aggregate.load_aggregated import ACSAggregator

access_to_carcommute = ACSAggregator(
    name="access_carcommute",
    dcp_base_variables=["access_carcommute", "access_workers16pl"],
    dcp_base_variables_conf={
        "include_race": True,
        "include_year_in_out_col": True,
    },
    internal_review_filename="access_carcommute.csv",
    internal_review_category="quality_of_life",
    include_start_year=True,
)

access_transit_car = access_to_carcommute.run
