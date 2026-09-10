import json

import pandas as pd


def df_to_json(df: pd.DataFrame) -> str:
    """Converts pandas dataframe to pretty json
    Prints as json array rather than standard to_json"""
    return json.dumps(json.loads(df.to_json(orient="records")), indent=4)
