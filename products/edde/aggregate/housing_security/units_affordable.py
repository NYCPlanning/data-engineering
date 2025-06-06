from aggregate.load_aggregated import ACSAggregator

units_affordable_aggregator = ACSAggregator(
    name="units_affordable",
    dcp_base_variables=[
        "units_affordable_eli",
        "units_affordable_vli",
        "units_affordable_li",
        "units_affordable_mi",
        "units_affordable_midi",
        "units_affordable_hi",
    ],
    dcp_base_variables_conf={
        "include_race": False,
        "include_year_in_out_col": False,
    },
    internal_review_filename="units_affordable.csv",
    internal_review_category="housing_security",
    include_start_year=False,
)

units_affordable = units_affordable_aggregator.run
