from __future__ import annotations

import re
from abc import abstractmethod
from dataclasses import dataclass, field, fields, is_dataclass
from datetime import date
from enum import IntEnum, StrEnum
from functools import total_ordering

from dateutil.relativedelta import relativedelta
from pydantic import BaseModel


class DateVersionFormat(StrEnum):
    fiscal_year = "Fiscal Year"
    quarter = "Quarter"
    month = "Month"
    date = "Date"


class CapitalBudgetRelease(IntEnum):
    prelim = 1
    exec = 2
    adopt = 3


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


@total_ordering
class Version:
    @property
    @abstractmethod
    def label(self) -> str:
        raise NotImplementedError("Version is an abstract class")

    def __lt__(self, other) -> bool:
        raise NotImplementedError("Version is an abstract class")

    def __eq__(self, other) -> bool:
        raise NotImplementedError("Version is an abstract class")


@dataclass
class CapitalBudget(Version):
    year: int
    release: CapitalBudgetRelease
    patch: int = 0

    @property
    def release_name(self) -> str:
        return self.release.name

    @property
    def label(self) -> str:
        if self.patch == 0:
            return f"{self.year}{self.release_name}"
        else:
            return f"{self.year}{self.release_name}.{self.patch}"

    def __lt__(self, other) -> bool:
        match other:
            case CapitalBudget():
                return (self.year, self.release, self.patch) < (
                    other.year,
                    other.release,
                    other.patch,
                )
            case Date():
                self_year = self.year + 2000
                if self_year != other.date.year:
                    return self_year < other.date.year
                else:
                    raise ValueError(
                        "Cannot compare Date and CapitalBudget versions of the same year"
                    )
            case _:
                raise TypeError(f"Cannot compare Version with type '{type(other)}'")

    def __eq__(self, other) -> bool:
        match other:
            case CapitalBudget():
                return (self.year, self.release, self.patch) == (
                    other.year,
                    other.release,
                    other.patch,
                )
            case _:
                return False


@dataclass
class MajorMinor(Version):
    year: int
    major: int
    minor: int = 0
    patch: int = 0

    @property
    def label(self) -> str:
        if self.minor == 0 and self.patch == 0:
            return f"{self.year}v{self.major}"
        elif self.minor != 0 and self.patch == 0:
            return f"{self.year}v{self.major}.{self.minor}"
        else:
            return f"{self.year}v{self.major}.{self.minor}.{self.patch}"

    def __lt__(self, other) -> bool:
        match other:
            case MajorMinor():
                return (self.year, self.major, self.minor, self.patch) < (
                    other.year,
                    other.major,
                    other.minor,
                    other.patch,
                )
            case Date():
                self_year = self.year + 2000
                if self_year != other.date.year:
                    return self_year < other.date.year
                else:
                    raise ValueError(
                        "Cannot compare Date and MajorMinor versions of the same year"
                    )
            case _:
                raise TypeError(f"Cannot compare Version with type '{type(other)}'")

    def __eq__(self, other) -> bool:
        match other:
            case MajorMinor():
                return (self.year, self.major, self.minor, self.patch) == (
                    other.year,
                    other.major,
                    other.minor,
                    other.patch,
                )
            case _:
                return False


@dataclass
class Date(Version):
    date: date
    format: DateVersionFormat
    patch: int = 0

    @property
    def label(self) -> str:
        match self.format:
            case DateVersionFormat.fiscal_year:
                if self.patch == 0:
                    return f"FY{self.date.strftime('%Y')}"
                else:
                    return f"FY{self.date.strftime('%Y')}.{self.patch}"
            case DateVersionFormat.quarter:
                if self.patch == 0:
                    return (
                        f"{self.date.strftime('%y')}Q{int((self.date.month + 2) / 3)}"
                    )
                else:
                    return f"{self.date.strftime('%y')}Q{int((self.date.month + 2) / 3)}.{self.patch}"
            case DateVersionFormat.month:
                if self.patch == 0:
                    return self.date.strftime("%Y-%m")
                else:
                    return f"{self.date.strftime('%Y-%m')}.{self.patch}"
            case DateVersionFormat.date:
                if self.patch == 0:
                    return self.date.strftime("%Y-%m-%d")
                else:
                    return f"{self.date.strftime('%Y-%m-%d')}.{self.patch}"
            case _:
                raise ValueError(f"Unsupported DateVersionFormat '{self.format}'")

    def __lt__(self, other) -> bool:
        match other:
            case Date():
                return (self.date, self.format, self.patch) < (
                    other.date,
                    other.format,
                    other.patch,
                )
            case MajorMinor():
                other_year = other.year + 2000
                if self.date.year != other_year:
                    return self.date.year < other_year
                else:
                    raise ValueError(
                        "Cannot compare Date and MajorMinor versions of the same year"
                    )
            case CapitalBudget():
                other_year = other.year + 2000
                if self.date.year != other_year:
                    return self.date.year < other_year
                else:
                    raise ValueError(
                        "Cannot compare Date and CapitalBudget versions of the same year"
                    )
            case _:
                raise TypeError(f"Cannot compare Version with type '{type(other)}'")

    def __eq__(self, other) -> bool:
        match other:
            case Date():
                return (self.date, self.format, self.patch) == (
                    other.date,
                    other.format,
                    other.patch,
                )
            case _:
                return False


def generate_first_of_month() -> Date:
    return Date(date=date.today().replace(day=1), format=DateVersionFormat.date)


class BumpLatestRelease(BaseModel):
    bump_latest_release: int


class PinToSourceDataset(BaseModel):
    pin_to_source_dataset: str


class SimpleVersionStrategy(StrEnum):
    first_of_month = "first_of_month"
    bump_latest_release = "bump_latest_release"


VersionStrategy = SimpleVersionStrategy | BumpLatestRelease | PinToSourceDataset


class VersionSubType(StrEnum):
    major = "major"
    minor = "minor"
    patch = "patch"


def parse(v: str) -> Version:
    """Takes a version string and attempts to parse it into one of our defined version types"""
    match RegexSpmMatch(v):
        case r"^(\d{2})v(\d+)$" as m:
            return MajorMinor(year=int(m[1]), major=int(m[2]))
        case r"^(\d{2})v(\d+)\.(\d+)$" as m:
            return MajorMinor(year=int(m[1]), major=int(m[2]), minor=int(m[3]))
        case r"^(\d{2})v(\d+)\.(\d+)\.(\d+)$" as m:
            return MajorMinor(
                year=int(m[1]), major=int(m[2]), minor=int(m[3]), patch=int(m[4])
            )
        case r"^FY(\d{4})$" as m:
            return Date(
                date(int(m[1]), 1, 1),
                format=DateVersionFormat.fiscal_year,
            )
        case r"^(\d{2})Q(\d)$" as m:
            return Date(
                date(2000 + int(m[1]), int(m[2]) * 3 - 2, 1),
                format=DateVersionFormat.quarter,
            )
        case r"^(\d{2})Q(\d)\.(\d+)$" as m:
            return Date(
                date(2000 + int(m[1]), int(m[2]) * 3 - 2, 1),
                format=DateVersionFormat.quarter,
                patch=int(m[3]),
            )
        case r"^(\d{4})-(\d{2})-(\d{2})$" as m:
            return Date(
                date(int(m[1]), int(m[2]), int(m[3])),
                format=DateVersionFormat.date,
            )
        case r"^(\d{4})-(\d{2})-(\d{2})\.(\d+)$" as m:
            return Date(
                date(int(m[1]), int(m[2]), int(m[3])),
                format=DateVersionFormat.date,
                patch=int(m[4]),
            )
        case r"^(\d{4})-(\d{2})$" as m:
            return Date(
                date(int(m[1]), int(m[2]), 1),
                format=DateVersionFormat.month,
            )
        case r"^(\d{4})-(\d{2}).(\d+)$" as m:
            return Date(
                date(int(m[1]), int(m[2]), 1),
                format=DateVersionFormat.month,
                patch=int(m[3]),
            )
        case r"^(\d{2})(adopt|exec|prelim)$" as m:
            return CapitalBudget(
                year=int(m[1]),
                release=CapitalBudgetRelease[m[2]],
            )
        case r"^(\d{2})(adopt|exec|prelim)\.(\d+)$" as m:
            return CapitalBudget(
                year=int(m[1]),
                release=CapitalBudgetRelease[m[2]],
                patch=int(m[3]),
            )
        case _:
            raise ValueError(
                f"Tried to parse version {v} but it did not match the expected format"
            )


def sort(versions: list[Version]) -> list[Version]:
    """
    Sorts version in ascending order: from oldest to newest.
    """
    version_types = set([type(v) for v in versions])
    # can't compare different types
    if len(version_types) > 1:
        # only handle date-like versions
        raise TypeError(
            f"Can't sort mixed types of dataset versions: {[v.__name__ for v in version_types]}"
        )
    return sorted(versions)


def is_newer(version_1: str, version_2: str) -> bool:
    """
    Compares `version_1` to `version_2`. Returns True if `version_1` is newer than `version_2`.
    Both versions are expected to be of same Version subtype.
    """
    version_1_obj = parse(version_1)
    version_2_obj = parse(version_2)
    return version_1_obj > version_2_obj


def group_versions_by_base(version: str, versions_list: list[str]) -> list[str]:
    """
    Groups versions by their base, ignoring patch differences, and returns a list of matching versions
    in ascending order (from oldest to newest).

    Example:

    ```python
    version = '24Q1'
    versions_list = ['24Q1', '24Q1.1', '24v3', '24v3.0.1', '23Q1']

    matching_versions = group_versions_by_base(version, versions_list)
    print(matching_versions)  # Output: ['24Q1', '24Q1.1']
    ```
    """
    parsed_version = parse(version)
    same_version_type_lst = [
        v for v in versions_list if isinstance(parse(v), type(parsed_version))
    ]

    def is_matching_version(base_version: Version, compare_version: Version) -> bool:
        """Helper function to check if compare_version matches base_version except for the patch."""
        assert is_dataclass(base_version) and is_dataclass(
            compare_version
        )  # This line is to appease mypy

        # Get Version key-value pairs excluding "patch" attribute to use for comparison
        base_fields = {
            field.name: getattr(base_version, field.name)
            for field in fields(base_version)
            if field.name != "patch"
        }
        compare_fields = {
            field.name: getattr(compare_version, field.name)
            for field in fields(compare_version)
            if field.name != "patch"
        }
        return base_fields == compare_fields

    # Filter versions for matching attributes (excluding patch)
    matching_versions = [
        v
        for v in same_version_type_lst
        if is_matching_version(parsed_version, parse(v))
    ]

    return sorted(matching_versions)


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
        case MajorMinor(), VersionSubType.minor:
            return MajorMinor(
                year=previous_version.year,
                major=previous_version.major,
                minor=previous_version.minor + bump_by,
            )
        case MajorMinor(), (VersionSubType.major | None):
            return MajorMinor(
                year=previous_version.year, major=previous_version.major + bump_by
            )
        case MajorMinor(), VersionSubType.patch:
            return MajorMinor(
                year=previous_version.year,
                major=previous_version.major,
                minor=previous_version.minor,
                patch=previous_version.patch + bump_by,
            )
        case Date(format=DateVersionFormat.fiscal_year), None:
            return Date(
                date=previous_version.date + relativedelta(years=bump_by),
                format=previous_version.format,
            )
        case Date(format=DateVersionFormat.quarter), None:
            return Date(
                date=previous_version.date + relativedelta(months=bump_by * 3),
                format=previous_version.format,
            )
        case Date(format=DateVersionFormat.quarter), VersionSubType.patch:
            return Date(
                date=previous_version.date,
                format=previous_version.format,
                patch=previous_version.patch + bump_by,
            )
        case Date(format=DateVersionFormat.month), None:
            return Date(
                date=previous_version.date + relativedelta(months=bump_by),
                format=previous_version.format,
            )
        case Date(format=DateVersionFormat.month), VersionSubType.patch:
            return Date(
                date=previous_version.date,
                format=previous_version.format,
                patch=previous_version.patch + bump_by,
            )
        case Date(format=DateVersionFormat.date), VersionSubType.patch:
            return Date(
                date=previous_version.date,
                format=previous_version.format,
                patch=previous_version.patch + bump_by,
            )
        case Date(format=DateVersionFormat.date), None:
            raise NotImplementedError("Date version cannot be bumped except for patch")
        case Date(), None:
            raise ValueError(f"Unsupported date format {previous_version.format}")
        case Date(), _:
            raise ValueError(
                f"Version subtype {bump_type} not applicable for Date versions"
            )
        case CapitalBudget(), None:
            total_bumped_value = previous_version.release + bump_by
            bumped_release = (total_bumped_value - 1) % 3 + 1
            bumped_year = previous_version.year + ((total_bumped_value - 1) // 3)

            return CapitalBudget(
                year=bumped_year,
                release=CapitalBudgetRelease(bumped_release),
            )
        case CapitalBudget(), VersionSubType.patch:
            return CapitalBudget(
                year=previous_version.year,
                release=previous_version.release,
                patch=previous_version.patch + bump_by,
            )
        case _:
            raise ValueError(f"Unsupported version format {previous_version}")


@dataclass(order=True)
class DraftVersionRevision:
    revision_num: int
    revision_summary: str = field(compare=False)

    def __post_init__(self):
        max_string_length = 50  # random max length. may need to revise

        if not isinstance(self.revision_num, int) or self.revision_num <= 0:
            raise ValueError(
                f"revision_num must be a positive integer, got {self.revision_num}"
            )
        if len(self.revision_summary) > max_string_length:
            raise ValueError(
                f"revision_summary must be no more than {max_string_length} characters, got {len(self.revision_summary)} characters"
            )

    @property
    def label(self) -> str:
        if self.revision_summary == "":
            return f"{self.revision_num}"
        else:
            return f"{self.revision_num}-{self.revision_summary}"


def parse_draft_version(v: str) -> DraftVersionRevision:
    """Takes a version string and attempts to parse it into DraftVersionRevision object."""
    revision_num_str, *rest = v.split("-", 1)
    revision_summary = rest[0] if len(rest) == 1 else ""
    try:
        revision_num = int(revision_num_str)
    except ValueError:
        raise ValueError(f"Unsupported draft version revision format {v}")

    return DraftVersionRevision(revision_num, revision_summary)
