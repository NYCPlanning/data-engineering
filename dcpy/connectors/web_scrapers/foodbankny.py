import re
from pathlib import Path

import requests

from dcpy.connectors.registry import Pull
from dcpy.utils.logging import logger

BASE_MAP_URL = "https://fbnycmap.z13.web.core.windows.net/"


class FoodBankNYConnector(Pull):
    conn_type: str = "foodbankny"
    filename: str = "customsearch_agency_map_data.csv"

    def push(self, key: str, **kwargs) -> dict:
        raise Exception("Can't push to this source")

    def pull(
        self,
        key: str,
        destination_path: Path,
        **kwargs,
    ) -> dict:
        logger.info(f"Pulling NYC Food Bank locations from {BASE_MAP_URL}")
        resp = requests.get(BASE_MAP_URL)
        resp.raise_for_status()
        match = re.search(r'csvUrl\s=\s"([^"]+)"', resp.text)

        if not match:
            raise Exception(
                "Could not find csvUrl in the base page, formatting has likely changed"
            )

        csv_resp = requests.get(match.group(1))
        csv_resp.raise_for_status()

        filepath = destination_path / self.filename
        logger.info(f"Saving FoodBankNYC location data to {filepath}")
        with open(filepath, "wb") as f:
            f.write(csv_resp.content)
        return {"path": filepath}
