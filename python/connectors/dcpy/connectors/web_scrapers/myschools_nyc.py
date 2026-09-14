import json
from pathlib import Path
from typing import Any

import requests

from dcpy.connectors.registry import Pull
from dcpy.utils.logging import logger


def scrape_schools_api(base_url) -> list[dict[str, Any]]:
    """
    Scrape all results from the paginated NYC Schools API.

    Should work for any similar json-returning paginated API
    that uses 'next' links for pagination and 'results' for data.
    """
    all_results = []
    current_url = base_url
    page_count = 0

    logger.info(f"Starting scrape from: {base_url}")

    while current_url:
        page_count += 1
        logger.info(f"Fetching page {page_count}: {current_url}")

        response = requests.get(current_url, timeout=30)
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])

        if not results:
            break

        all_results.extend(results)

        current_url = data.get("next")

    logger.info(
        f"Scraping complete. Fetched {len(all_results)} records from {page_count} pages."
    )

    return all_results


class MySchoolsNYCConnector(Pull):
    conn_type: str = "myschools_nyc"
    filename: str = "myschools_nyc.json"

    # I believe process/5 is specific to prek? Have not explored the full api
    url: str = "https://www.myschools.nyc/en/api/v2/schools/process/5/"

    def pull(
        self,
        key: str,
        destination_path: Path,
        **kwargs,
    ) -> dict:
        # TODO if we expand this to other endpoints, make this a dict instead of hardcoded
        if key == "prek":
            scrape_results = scrape_schools_api(self.url)
        else:
            raise NotImplementedError(
                f"{key} not implemented for MySchools NYC connector, only 'prek'"
            )

        filename = self.conn_type + "_" + key + ".json"
        filepath = destination_path / filename
        logger.info(f"Saving MySchools NYC data to {filepath}")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(scrape_results, f, indent=2, ensure_ascii=False)
            return {"path": filepath}
