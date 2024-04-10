"""Write one or more .csv files to be pushed to github for internal review"""

from typing import List, Tuple
import pandas as pd
import os


def set_internal_review_files(data: List[Tuple[pd.DataFrame, str, str]], category):
    """Save list of dataframes as csv."""
    for df, name, geography in data:
        print(f"Writing {name} to internal review folder")
        df.to_csv(f"internal_review/{category}/{geography}/{name}")
