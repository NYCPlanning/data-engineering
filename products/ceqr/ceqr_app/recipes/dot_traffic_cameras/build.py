import sys

import pandas as pd
import requests

# Get locations and names of cameras from popups on NYCDOT's google map
user_agent = {"User-agent": "Mozilla/5.0"}
r = requests.get(
    "https://webcams.nyctmc.org/new-data.php?query=", headers=user_agent
).json()
df = pd.DataFrame(r["markers"])
df["url"] = "https://webcams.nyctmc.org/google_popup.php?cid=" + df.id

# Output raw data
df.to_csv(
    "output/raw.csv",
    columns=["content", "icon", "id", "latitude", "longitude", "title", "url"],
    index=False,
)

# Output data to stdout for transfer to EDM database
df.to_csv(
    sys.stdout,
    columns=["content", "icon", "id", "latitude", "longitude", "title", "url"],
    index=False,
)
