import numpy as np
import pandas as pd
from pipelines.acs_manual_update import transform_dataframe


def test_transform_dataframe_preserves_comma_formatted_estimates():
    """Population's spreadsheet stores top-coded medians as comma-formatted text
    (e.g. '200,000'). These must survive ingest as numbers, not get coerced to NaN.
    Regression for the median bug in issue #2150."""
    df = pd.DataFrame(
        {
            "geotype": ["NTA2020"],
            "geoid": ["MN0102"],
            "vbarc": [8.5],
            "vbare": ["200,000"],  # comma-formatted estimate, currently coerced to NaN
            "vbarm": [np.nan],
            "vbarp": [np.nan],
            "vbarz": [np.nan],
        }
    )

    out = transform_dataframe(df)

    estimate = out.loc[out["pff_variable"] == "vbar", "e"].iloc[0]
    assert estimate == 200000
