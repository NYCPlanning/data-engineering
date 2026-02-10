import json
import tempfile

import pandas as pd
import requests


def df_to_tempfile(df: pd.DataFrame) -> str:
    f = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    df.to_csv(f, index=False)
    return f.name


def get_json_content(url: str):
    """
    Gets content of a JSON file from a URL.
    url: URL of JSON file
        exmaple: https://refinery.nypl.org/api/nypl/locations/v1.0/locations
        exmaple: https://www.nycgovparks.org/bigapps/DPR_CapitalProjectTracker_001.json
    """
    # use default headers to prevent "403 Client Error: Forbidden for url"
    default_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    }
    response = requests.get(url, headers=default_headers)
    response.raise_for_status()  # raises exception when not a 2xx response
    content = json.loads(response.content)
    return content
