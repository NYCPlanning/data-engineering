from aggregate import load_aggregated

units_affordable_aggregator = load_aggregated.ACSAggregator(
    name="units_affordable",
    dcp_base_variables=[
        "units_affordable_eli",
        "units_affordable_vli",
        "units_affordable_li",
        "units_affordable_mi",
        "units_affordable_midi",
        "units_affordable_hi",
    ],
    internal_review_filename="units_affordable.csv",
    internal_review_category="housing_security",
)

units_affordable = units_affordable_aggregator.run
