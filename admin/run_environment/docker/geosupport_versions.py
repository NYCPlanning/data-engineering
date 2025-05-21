# Get the relevant release details from the Geosupport Open Data page
# and set an environment variable
import sys
import json
import logging
import re
import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level="INFO",
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

CALLER_ENVIRONMENT_VARIABLE_NAME = "VERSIONSTRING"
GEOSUPPORT_RELEASE_URL = "https://apps.nyc.gov/content-api/v1/content/planning/resources/geocoding/geosupport-desktop-edition"
# to convert a release letter to a number for the docker image tag
MINOR_LETTER_LOOKUP = {
    "a": 1,
    "b": 2,
    "c": 3,
    "d": 4,
}


def get_release_from_section(soup: BeautifulSoup, section: str) -> str:
    header = soup.find("h2", string=section)
    assert header, f"No h2 with text '{section}' found"
    p = header.find_next_sibling()
    assert p and p.name == "p", "No p section found under header"
    regex = re.search("Latest Release: ([0-9A-D]+)\\n", p.text)
    assert regex, "Release text not found"
    return regex.groups()[0].lower()


if __name__ == "__main__":
    content = json.loads(requests.get(GEOSUPPORT_RELEASE_URL).content)
    soup = BeautifulSoup(content["description"], features="html.parser")
    primary_release = get_release_from_section(soup, "Geosupport Desktop")
    upad_release = get_release_from_section(soup, "TPAD / UPAD")

    upad_primary_release = upad_release[:3]  # trim 25A4 to 25A

    logging.info(f"{primary_release=}")
    logging.info(f"{upad_release=}")
    logging.info(f"{upad_primary_release=}")

    if primary_release == upad_primary_release:
        logging.info("Matching Primary and UPAD's Primary releases")
        # UPAD should be incorporated
        release = upad_release
    else:
        logging.info("WARNING! Mismatch between posted Primary and UPAD releases")
        logging.info("Ignoring UPAD release")
        release = primary_release

    logging.info(f"{release=}")
    patch = 0 if len(release) == 3 else release[3]
    versions = dict(
        RELEASE=release[:3],
        MAJOR=release[:2],
        MINOR=MINOR_LETTER_LOOKUP.get(release[2]),
        PATCH=0,
    )

    version_string = f"RELEASE={versions['RELEASE']} MAJOR={versions['MAJOR']} MINOR={versions['MINOR']} PATCH={versions['PATCH']}"
    logging.info(f"Geosupport VERSIONSTRING from is {version_string}")
    print(version_string, file=sys.stdout)
