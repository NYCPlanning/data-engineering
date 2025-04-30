from typing import List, Tuple
import pandas as pd
from pathlib import Path


def set_internal_review_files(data: List[Tuple[pd.DataFrame, str, str]], category):
    """Save list of dataframes as csv."""
    for df, name, geography in data:
        print(f"Writing {name} to internal review folder")
        dir = Path("internal_review") / category / geography
        dir.mkdir(exist_ok=True, parents=True)
        df.to_csv(dir / name)
