from bs4 import BeautifulSoup
from pathlib import Path
import pandas as pd
import requests

from dcpy.connectors.web_scrapers import _address
from dcpy.connectors.registry import Pull
from dcpy.utils.logging import logger

BASE_URL = "https://www.uscourts.gov/federal-court-finder/find"


def _nyc_query_params():
    return {
        "coordinates": "40.7127753,-74.0059728",
        "country": "US",
        "county": "New York",
        "location": "New York, NY, USA",
        "state": "NY",
        "zip": "10007",
        "f[0]": "usc_district_id:1303",
    }


def _brooklyn_query_params():
    return {
        "coordinates": "40.6781784,-73.9441579",
        "country": "US",
        "county": "Kings",
        "location": "Brooklyn, NY, USA",
        "state": "NY",
        "zip": "11216",
        "f[0]": "usc_district_id:1301",
    }


def _get_nyc_links(params):
    logger.info(f"Querying court locations links for {params['location']}")
    all_links = []
    page_num = 0
    while True:
        params["page"] = page_num

        response = requests.get(BASE_URL, params=params)
        soup = BeautifulSoup(response.text, "html.parser")
        court_divs = soup.find_all("div", class_="court-finder-result")
        links = [d.find("a").get("href") for d in court_divs]
        if links:
            all_links += links
            page_num += 1
        else:
            break
    return all_links


def _fetch_location_info(link: str):
    logger.info(f"getting {link}")
    html = requests.get(link)
    soup = BeautifulSoup(html.text, "html.parser")

    # Extract the court name
    court_name_tag = soup.find("h1", class_="page-title")
    court_name = court_name_tag.get_text(strip=True) if court_name_tag else None

    # Extract the address
    address_tag = soup.find("address", class_="court-finder-court__address")
    address_text = (
        address_tag.get_text(separator=" ", strip=True) if address_tag else ""
    )
    parsed_address = (
        _address.parse(address_text) if address_text else _address.AddressComponents()
    )

    # Extract the court type
    court_type_tag = soup.find("div", class_="court-finder-court__court-type")
    court_type_text = court_type_tag.get_text(strip=True) if court_type_tag else None
    court_type = (
        court_type_text.replace("Court type:", "").strip() if court_type_text else None
    )

    # Extract the phone number
    phone_tag = soup.find("a", class_="court-finder-court__button-phone")
    phone_number = phone_tag.get_text(strip=True) if phone_tag else None

    return {
        "name": court_name,
        "courttype": court_type,
        "phone_number": phone_number,
        "place_name": parsed_address.place_name,
        "address": parsed_address.address_line_1 or parsed_address.building_name,
        "city": parsed_address.city,
        "state": parsed_address.state,
        "zip_code": parsed_address.zip_code,
        "raw_full_address": address_text,
    }


class USCourtsConnector(Pull):
    conn_type: str = "uscourts"
    filename: str = "uscourts_nyc_courts.csv"

    def pull(
        self,
        key: str,
        destination_path: Path,
        **kwargs,
    ) -> dict:
        """Some of the fields we require (like court type) aren't shown in the results
        So we basically just follow the links provided on the search page,
        and fetch the individual pages.
        """
        nyc_court_links = _get_nyc_links(_nyc_query_params()) + _get_nyc_links(
            _brooklyn_query_params()
        )
        df = pd.DataFrame([_fetch_location_info(link) for link in nyc_court_links])
        filepath = destination_path / self.filename
        logger.info(f"Saving US Federal Courts NYC location data to {filepath}")
        df.to_csv(filepath, index=False)
        return {"path": filepath}
