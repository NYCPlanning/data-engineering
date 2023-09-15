# Get the relevant release details from the Geosupport Open Data page
# and set an environment variable
import sys
import logging
import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level="INFO",
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

IGNORE_UPAD_RELEASE = (
    True  # TODO this is temporary while an image with 22c4 UPAD needs to be built
)

CALLER_ENVIRONMENT_VARIABLE_NAME = "VERSIONSTRING"
GEOSUPPORT_RELEASE_URL = (
    "https://www1.nyc.gov/site/planning/data-maps/open-data/dwn-gde-home.page"
)
# to convert a release letter to a number for the docker image tag
MINOR_LETTER_LOOKUP = {
    "a": 1,
    "b": 2,
    "c": 3,
    "d": 4,
}

if __name__ == "__main__":
    soup = BeautifulSoup(
        requests.get(GEOSUPPORT_RELEASE_URL).content, features="html.parser"
    )
    table = soup.find_all("table", class_="table table-outline-border")[0]
    releases = [
        i.string.replace("Release", "").strip().lower()
        for i in table.find_all("th")
        if "Release" in i.string
    ]

    logging.info(f"Release titles from Open Data table: {releases}")

    if len(releases) == 1:
        # only one release section present
        # no UPAD to incorporate
        release = releases[0]
        versions = dict(
            RELEASE=release[:3],
            MAJOR=release[:2],
            MINOR=MINOR_LETTER_LOOKUP.get(release[2]),
            PATCH=0,
        )
    else:
        # If more than 1 item in release
        # then there must be a UPAD present
        # Check if they are the same release
        primary_release = releases[0]
        upad_release = releases[1].split(" ")[-1]  # expecting "upad / tpad  22c4"
        upad_primary_release = upad_release[0:3]

        logging.info(f"{primary_release=}")
        logging.info(f"{upad_release=}")
        logging.info(f"{upad_primary_release=}")

        if primary_release == upad_primary_release:
            logging.info("Matching Primary and UPAD's Primary releases")
            # UPAD should be incorporated
            release = upad_release
        else:
            logging.info("WARNING! Mismatch between posted Primary and UPAD releases")
            # posted UPAD is not meant for current release
            # TODO this is temporary while an image with 22c4 UPAD needs to be built
            if IGNORE_UPAD_RELEASE:
                logging.info("Ignoring UPAD release")
                release = primary_release
            else:
                # build for the posted UPAD
                logging.info("Prioritizing UPAD release")
                release = upad_release

        logging.info(f"{release=}")
        if len(release) == 4:  # is a UPAD version
            versions = dict(
                RELEASE=release[:3],
                MAJOR=release[:2],
                MINOR=MINOR_LETTER_LOOKUP.get(release[2]),
                PATCH=release[3],
            )
        elif len(release) == 3:
            versions = dict(
                RELEASE=release[:3],
                MAJOR=release[:2],
                MINOR=MINOR_LETTER_LOOKUP.get(release[2]),
                PATCH=0,
            )
        else:
            raise ValueError(f"Got release string with unexpected length: {release=}")

    version_string = f"RELEASE={versions['RELEASE']} MAJOR={versions['MAJOR']} MINOR={versions['MINOR']} PATCH={versions['PATCH']}"
    print(version_string, file=sys.stdout)
