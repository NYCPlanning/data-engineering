from __future__ import annotations
from abc import abstractmethod
from dataclasses import dataclass
from datetime import date
from enum import Enum
from pydantic import BaseModel
import re


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

    @abstractmethod
    def bump(
        self, version_subtype: VersionSubType | None = None, bump_by: int | None = None
    ) -> Version:
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

    def bump(
        self, version_subtype: VersionSubType | None = None, bump_by: int | None = None
    ) -> MajorMinor:
        bump_by = bump_by or 1
        match version_subtype:
            case None:
                raise Exception("Must specify major or minor version to bump.")
            case VersionSubType.minor:
                return MajorMinor(
                    year=self.year, major=self.major, minor=self.minor + bump_by
                )
            case VersionSubType.major:
                return MajorMinor(year=self.year, major=self.major + bump_by)


@dataclass(order=True)
class Quarter(Version):
    year: int
    quarter: int

    @property
    def label(self) -> str:
        return f"{self.year}Q{self.quarter}"

    def bump(
        self, version_subtype: VersionSubType | None = None, bump_by: int | None = None
    ) -> Quarter:
        if version_subtype is not None:
            raise Exception(
                f"Version subtype {version_subtype} not applicable for Quarterly versions"
            )
        bump_by = bump_by or 1
        new_year = int(self.year + (self.quarter + bump_by - 1) / 4)
        new_quarter = ((self.quarter + bump_by - 1) % 4) + 1
        return Quarter(year=new_year, quarter=new_quarter)


@dataclass(order=True)
class FirstOfMonth(Version):
    year: int
    month: int

    @property
    def label(self) -> str:
        return f"{self.year}-{self.month:02d}-01"

    def bump(
        self, version_subtype: VersionSubType | None = None, bump_by: int | None = None
    ) -> FirstOfMonth:
        if version_subtype is not None:
            raise Exception(
                f"Version subtype {version_subtype} not applicable for FirstOfMonth versions"
            )
        bump_by = bump_by or 1
        new_year = int(self.year + (self.month + bump_by - 1) / 12)
        new_month = ((self.month + bump_by - 1) % 12) + 1
        return FirstOfMonth(year=new_year, month=new_month)

    @staticmethod
    def generate() -> FirstOfMonth:
        version = parse(date.today().strftime("%Y-%m-01"))
        match version:
            case FirstOfMonth():
                return version
            case _:
                raise Exception("Version parsing failed")


@dataclass(order=True)
class Today(Version):
    year: int
    month: int
    day: int

    @property
    def label(self) -> str:
        return f"{self.year}-{self.month:02d}-{self.day:02d}"

    def bump(
        self, version_subtype: VersionSubType | None = None, bump_by: int | None = None
    ) -> Today:
        if version_subtype is not None:
            raise Exception(
                f"Version subtype {version_subtype} not applicable for Today versions"
            )
        bump_by = bump_by or 1
        new_year = int(self.year + (self.month + bump_by - 1) / 12)
        new_month = int(
            self.month + (self.day + bump_by - 1) / 31
        )  # not every month has 31 days!
        new_day = ((self.day + bump_by - 1) % 31) + 1
        return Today(year=new_year, month=new_month, day=new_day)

    @staticmethod
    def generate() -> Today:
        version = parse(date.today().strftime("%Y-%m-%d"))
        match version:
            case Today():
                return version
            case _:
                raise Exception("Version parsing failed")


class BumpLatestRelease(BaseModel):
    bump_latest_release: int


class SimpleVersionStrategy(str, Enum):
    today = "today"
    first_of_month = "first_of_month"
    bump_latest_release = "bump_latest_release"


VersionStrategy = SimpleVersionStrategy | BumpLatestRelease


class VersionSubType(str, Enum):
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
            return Quarter(year=int(m[1]), quarter=int(m[2]))
        case r"^(\d{4})-(\d{2})-01$" as m:
            return FirstOfMonth(year=int(m[1]), month=int(m[2]))
        case r"^(\d{4})-(\d{2})-(\d{2})" as m:
            return Today(year=int(m[1]), month=int(m[2]), day=int(m[3]))
        case _:
            raise ValueError(
                f"Tried to parse version {v} but it did not match the expected format"
            )


def bump(
    previous_version: str,
    bump_type: VersionSubType | None = None,
    bump_by: int | None = None,
) -> str:
    """Takes a version string, attempts to parse it, bumps it, and returns the string label of the bumped version"""
    version = parse(previous_version)
    bumped = version.bump(bump_type, bump_by=bump_by)
    return bumped.label
