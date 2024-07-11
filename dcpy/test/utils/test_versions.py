from datetime import date
from unittest import TestCase

from dcpy.utils import versions


class TestVersions(TestCase):
    def test_parsing_valid_versions(self):
        for version, parsed in [
            ["23v2", versions.MajorMinor(year=23, major=2)],
            ["23v2.11", versions.MajorMinor(year=23, major=2, minor=11)],
            ["24v34.56", versions.MajorMinor(year=24, major=34, minor=56)],
            [
                "2020-03-28",
                versions.Date(
                    date=date(2020, 3, 28),
                    format=versions.DateVersionFormat.date,
                ),
            ],
            [
                "23Q4",
                versions.Date(
                    date=date(2023, 10, 1),
                    format=versions.DateVersionFormat.quarter,
                ),
            ],
        ]:
            self.assertEqual(parsed, versions.parse(version))

    def test_parsing_invalid_version(self):
        with self.assertRaises(Exception):
            versions.parse("23v")
        with self.assertRaises(Exception):
            versions.parse("2v12")
        with self.assertRaises(Exception):
            versions.parse("20231212")

    def test_sort_valid_versions(self):
        for version_list, sorted in [
            [
                [
                    "23v2",
                    "23v1",
                    "22v3.1",
                    "22v3",
                ],
                [
                    "22v3",
                    "22v3.1",
                    "23v1",
                    "23v2",
                ],
            ],
            [
                [
                    "23Q4",
                    "2023-10-02",
                    "2023-11-02",
                    "2023-11",
                    "2023-02-05",
                    "2024-01-01",
                    "23Q2",
                ],
                [
                    "2023-02-05",
                    "23Q2",
                    "23Q4",
                    "2023-10-02",
                    "2023-11",
                    "2023-11-02",
                    "2024-01-01",
                ],
            ],
        ]:
            parsed_and_sorted = versions.sort([versions.parse(v) for v in version_list])
            labels = [v.label for v in parsed_and_sorted]
            self.assertEqual(sorted, labels)

    def test_sort_invalid_versions(self):
        with self.assertRaises(TypeError):
            versions.sort(
                [
                    versions.Date(
                        date(2024, 1, 1), format=versions.DateVersionFormat.quarter
                    ),
                    versions.MajorMinor(year=23, major=2),
                ]
            )

    def test_bumping_versions(self):
        for bumped_part, bump_by, v, v_expected in [
            ["major", None, "23v2", "23v3"],
            ["major", None, "23v2.1", "23v3"],
            ["minor", None, "23v2", "23v2.1"],
            ["minor", None, "23v2.1", "23v2.2"],
            [None, 1, "23Q1", "23Q2"],
            [None, 2, "23Q4", "24Q2"],
            [None, 7, "23Q2", "25Q1"],
            [None, 7, "2023-01", "2023-08"],
            [None, 1, "2023-12", "2024-01"],
        ]:
            self.assertEqual(v_expected, versions.bump(v, bumped_part, bump_by).label)
