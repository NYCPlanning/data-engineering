from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from dcpy.connectors.registry import Pull
from dcpy.connectors.web_scrapers import _address
from dcpy.utils.logging import logger

URL = "https://www.queenslibrary.org/about-us/locations/?view=all"


def _soup_try_find_text(section, **kwargs):
    div = section.find("div", **kwargs)
    return div.get_text(strip=True) if div else ""


def _soup_find(soup: BeautifulSoup | Tag, tag: str, **kwargs) -> Tag:
    found = soup.find(tag, **kwargs)
    assert isinstance(found, Tag)
    return found


def _fetch_location_info(url: str) -> dict:
    logger.info(f"getting {url}")
    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")

    name_header = soup.find("h1", class_="page-header")
    assert name_header
    name = name_header.get_text(strip=True)

    address_tag = _soup_find(soup, "div", id="address")
    address_text = address_tag.get_text() if address_tag else ""
    parsed_address = _address.parse(address_text)

    phone = _soup_try_find_text(soup, id="phone")
    dropoff = _soup_try_find_text(soup, id="dropoff")
    mobility = _soup_try_find_text(soup, id="mobility")

    # Extract about text
    about_section = _soup_find(soup, "h2", string="About This Location")
    parent = about_section.find_parent()
    paragraphs = parent.find_all("p") if parent else []
    about = " ".join([p.get_text(strip=True) for p in paragraphs])

    hours = {}
    # Extract hours
    for div in soup.find_all("div", class_="office-hours__item") or []:
        label = _soup_find(div, "span", class_="office-hours__item-label").get_text()
        label = label.strip(": ")
        hours_spans = div.find_all("span", class_="office-hours__item-slots")
        hours_text = " - ".join([span.get_text(strip=True) for span in hours_spans])
        hours[label] = hours_text

    # Find the branch map section and extract lat/long
    latitude, longitude = None, None
    branch_map = _soup_find(soup, "section", id="block-branchmap")
    iframe = _soup_find(branch_map, "iframe")
    src = iframe.get("src", "")
    if isinstance(src, str):
        parts = src.split("&")
        if len(parts) >= 3:
            latitude = parts[1]
            longitude = parts[2]

    return {
        "name": name,
        "place_name": parsed_address.place_name,
        "address": parsed_address.address_line_1 or parsed_address.building_name,
        "city": parsed_address.city,
        "state": parsed_address.state,
        "zip_code": parsed_address.zip_code,
        "phone": phone,
        "dropoff": dropoff,
        "mobility": mobility,
        "sunday_hours": hours.get("Sunday"),
        "monday_hours": hours.get("Monday"),
        "tuesday_hours": hours.get("Tuesday"),
        "wednesday_hours": hours.get("Wednesday"),
        "thursday_hours": hours.get("Thursday"),
        "friday_hours": hours.get("Friday"),
        "saturday_hours": hours.get("Saturday"),
        "about": about,
        "latitude": latitude,
        "longitude": longitude,
    }


def _get_link_from_card(card) -> str | None:
    """Processed a 'card' html element and extracts library information."""
    section = card.find("section")
    if not section:
        return None

    title_p = section.find("p", class_="title")
    if not title_p:
        return None

    link = title_p.find("a").get("href")
    return f"https://www.queenslibrary.org{link}" if link else None


class QPLConnector(Pull):
    conn_type: str = "qpl"
    filename: str = "qpl_libraries.csv"

    def pull(
        self,
        key: str,
        destination_path: Path,
        **kwargs,
    ) -> dict:
        logger.info(f"Scraping QPL library locations links from {URL}")
        response = requests.get(URL)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")
        library_cards = soup.find_all("div", class_="locations-landing card")
        libraries = []

        all_links = [_get_link_from_card(card) for card in library_cards]
        # filter out none and virtual library
        links = [link for link in all_links if link and "anywhere" not in link]
        libraries = [_fetch_location_info(link) for link in links]

        df = pd.DataFrame(libraries)
        filepath = destination_path / self.filename
        logger.info(f"Saving QPL library data to {filepath}")
        df.to_csv(filepath, index=False)
        return {"path": filepath}
