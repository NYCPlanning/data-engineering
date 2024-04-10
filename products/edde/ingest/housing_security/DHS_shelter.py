"""Source data is from NYC open data"""


from datetime import datetime
import pandas as pd
import requests


def load_DHS_shelter(year: int) -> pd.DataFrame:
    url = "https://data.cityofnewyork.us/resource/ur7y-ziyb.json"
    timestamp = datetime(year, 3, 31).isoformat()
    query_str = f"?report_date={timestamp}"

    res = requests.get(f"{url}{query_str}")
    if res.status_code != 200:
        raise Exception("issue with DHS shelter GET request")
    return pd.DataFrame(res.json())
