from __future__ import annotations
from abc import abstractmethod
from dataclasses import dataclass
from datetime import date
from dateutil.relativedelta import relativedelta
from enum import StrEnum
from pydantic import BaseModel
import re


class DateVersionFormat(StrEnum):
    quarter = "Quarter"
    month = "Month"
    date = "Date"


@dataclass
class RegexSpmMatch:
    string: str
    match: re.Match | None = None

    def __eq__(self, pattern: object):
        if not isinstance(pattern, str):
            return False
        self.match = re.match(re.compile(pattern), self.string)
        return self.match is not None

    def __getitem__(self, group: int | str) -> str | None:
        if self.match is not None:
            return self.match[group]
        else:
            return None


@dataclass(order=True)
class Version:
    @property
    @abstractmethod
    def label(self) -> str:
        raise NotImplementedError("Version is an abstract class")


@dataclass(order=True)
class MajorMinor(Version):
    year: int
    major: int
    minor: int = 0

    @property
    def label(self) -> str:
        if self.minor == 0:
            return f"{self.year}v{self.major}"
        else:
            return f"{self.year}v{self.major}.{self.minor}"


@dataclass(order=True)
class Date(Version):
    date: date
    format: DateVersionFormat

    @property
    def label(self) -> str:
        match self.format:
            case DateVersionFormat.quarter:
                return f"{self.date.strftime("%y")}Q{int((self.date.month + 2) / 3)}"
            case DateVersionFormat.month:
                return self.date.strftime("%Y-%m")
            case DateVersionFormat.date:
                return self.date.strftime("%Y-%m-%d")


def generate_first_of_month() -> Date:
    return Date(date=date.today().replace(day=1), format=DateVersionFormat.date)


class BumpLatestRelease(BaseModel):
    bump_latest_release: int


class SimpleVersionStrategy(StrEnum):
    first_of_month = "first_of_month"
    bump_latest_release = "bump_latest_release"


VersionStrategy = SimpleVersionStrategy | BumpLatestRelease


class VersionSubType(StrEnum):
    major = "major"
    minor = "minor"


def parse(v: str) -> Version:
    """Takes a version string and attempts to parse it into one of our defined version types"""
    match RegexSpmMatch(v):
        case r"^(\d{2})v(\d+)$" as m:
            return MajorMinor(year=int(m[1]), major=int(m[2]))
        case r"^(\d{2})v(\d+)\.(\d+)$" as m:
            return MajorMinor(year=int(m[1]), major=int(m[2]), minor=int(m[3]))
        case r"^(\d{2})Q(\d)$" as m:
            return Date(
                date(2000 + int(m[1]), int(m[2]) * 3 - 2, 1),
                format=DateVersionFormat.quarter,
            )
        case r"^(\d{4})-(\d{2})-(\d{2})$" as m:
            return Date(
                date(int(m[1]), int(m[2]), int(m[3])),
                format=DateVersionFormat.date,
            )
        case r"^(\d{4})-(\d{2})$" as m:
            return Date(
                date(int(m[1]), int(m[2]), 1),
                format=DateVersionFormat.month,
            )
        case _:
            raise ValueError(
                f"Tried to parse version {v} but it did not match the expected format"
            )


def sort(versions: list[Version]) -> list[Version]:
    version_types = set([type(v) for v in versions])
    # can't compare different types
    if len(version_types) != 1:
        # only handle date-like versions
        raise TypeError(
            f"Can't sort mixed types of dataset versions: {[v.__name__ for v in version_types]}"
        )
    return sorted(versions)


def bump(
    previous_version: str | Version,
    bump_type: VersionSubType | None = None,
    bump_by: int | None = None,
) -> Version:
    """Takes a version string, attempts to parse it, bumps it, and returns the string label of the bumped version"""
    if isinstance(previous_version, str):
        previous_version = parse(previous_version)
    bump_by = bump_by or 1
    match previous_version, bump_type:
        case MajorMinor(), None:
            raise Exception("Must specify major or minor version to bump.")
        case MajorMinor(), VersionSubType.minor:
            return MajorMinor(
                year=previous_version.year,
                major=previous_version.major,
                minor=previous_version.minor + bump_by,
            )
        case MajorMinor(), VersionSubType.major:
            return MajorMinor(
                year=previous_version.year, major=previous_version.major + bump_by
            )
        
        case Date(format=DateVersionFormat.quarter), None:
            return Date(
                date=previous_version.date + relativedelta(months=bump_by * 3),
                format=previous_version.format,
            )
        case Date(format=DateVersionFormat.month), None:
            return Date(
                date=previous_version.date + relativedelta(months=bump_by),
                format=previous_version.format,
            )
        case Date(format=DateVersionFormat.date), None:
            raise NotImplementedError("Date version cannot be bumped")
        case Date(), None:
            raise ValueError(f"Unsupported date format {previous_version.format}")
        case Date(), _:
            raise Exception(
                f"Version subtype {bump_type} not applicable for Date versions"
            )
        
        case _:
            raise ValueError(f"Unsupported version format {previous_version}")
