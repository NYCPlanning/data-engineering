"""Shared version schedules: groups of templates whose versions follow one fixed calendar.

A template opts in with `version_schedule: <name>` at its top level. The schedule says what its
versions look like, so tools can list or check them without asking the source.
"""

import re
from enum import StrEnum


class VersionSchedule(StrEnum):
    # One release a quarter on the Geosupport schedule: two-digit year plus a quarter letter,
    # 26a through 26d for 2026. Lowercase, as Bytes names its files (nyct2020_26c.zip) and as
    # the archives in edm-recipes are versioned.
    geosupport_quarterly = "geosupport_quarterly"


_PATTERNS = {
    VersionSchedule.geosupport_quarterly: re.compile(r"^\d{2}[a-d]$"),
}

GEOSUPPORT_QUARTERS = "abcd"


def geosupport_quarterly_versions(first_year: int, last_year: int) -> list[str]:
    """Every quarterly version from `first_year`'s first quarter through `last_year`'s last,
    oldest first. Years are four-digit: (2025, 2026) -> 25a ... 26d."""
    if last_year < first_year:
        raise ValueError(f"last_year {last_year} is before first_year {first_year}")
    if first_year < 2000 or last_year > 2099:
        raise ValueError("years must be in 2000-2099 (versions use two digits)")
    return [
        f"{year % 100:02d}{quarter}"
        for year in range(first_year, last_year + 1)
        for quarter in GEOSUPPORT_QUARTERS
    ]


def versions(schedule: VersionSchedule, first_year: int, last_year: int) -> list[str]:
    """Every version on `schedule` for the years first_year through last_year, oldest first."""
    generators = {VersionSchedule.geosupport_quarterly: geosupport_quarterly_versions}
    return generators[schedule](first_year, last_year)


def is_valid_version(schedule: VersionSchedule, version: str) -> bool:
    """Whether `version` is written the way `schedule` writes its versions."""
    return bool(_PATTERNS[schedule].match(version))
