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
            ["26prelim", versions.CapitalBudget(year=26, release_num=1)],
            ["24exec", versions.CapitalBudget(year=24, release_num=2)],
            ["22adopt", versions.CapitalBudget(year=22, release_num=3)],
            ["25adopt.3", versions.CapitalBudget(year=25, release_num=3, patch=3)],
            ["25prelim", versions.CapitalBudget(year=25, release_num=1, patch=0)],
        ]:
            self.assertEqual(parsed, versions.parse(version))

    def test_parsing_invalid_version(self):
        with self.assertRaises(Exception):
            versions.parse("23v")
        with self.assertRaises(Exception):
            versions.parse("2v12")
        with self.assertRaises(Exception):
            versions.parse("20231212")
        with self.assertRaises(Exception):
            versions.parse("25PRELIM")
        with self.assertRaises(Exception):
            versions.parse("25executive")

    def test_sort_valid_versions(self):
        for version_list, sorted_list in [
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
                    "25v3",
                    "21v1",
                    "2023-11",
                    "2023-02-05",
                    "2024-01-01",
                    "23Q2",
                ],
                [
                    "21v1",
                    "2023-02-05",
                    "23Q2",
                    "23Q4",
                    "2023-10-02",
                    "2023-11",
                    "2023-11-02",
                    "2024-01-01",
                    "25v3",
                ],
            ],
            [
                [
                    "24exec",
                    "2023-03-10",
                    "25prelim",
                    "2023-05-24",
                    "2022-02-05",
                    "24prelim",
                ],
                [
                    "2022-02-05",
                    "2023-03-10",
                    "2023-05-24",
                    "24prelim",
                    "24exec",
                    "25prelim",
                ],
            ],
        ]:
            parsed_and_sorted = sorted([versions.parse(v) for v in version_list])
            labels = [v.label for v in parsed_and_sorted]
            self.assertEqual(sorted_list, labels)

    def test_sort_invalid_versions(self):
        with self.assertRaises(ValueError):
            [
                versions.Date(
                    date(2024, 1, 1), format=versions.DateVersionFormat.quarter
                ),
                versions.MajorMinor(year=24, major=2),
            ].sort()
        with self.assertRaises(TypeError):
            [
                versions.Date(
                    date(2024, 1, 1), format=versions.DateVersionFormat.quarter
                ),
                "2024-01-01",
            ].sort()
        with self.assertRaises(ValueError):
            [
                versions.CapitalBudget(year=25, release_num=1),
                versions.Date(date(2025, 1, 1), format=versions.DateVersionFormat.date),
            ].sort()

    def test_is_newer_valid_versions(self):
        for version_1, version_2, bool_expected in [
            ["23v2", "22v3.4", True],
            ["23Q1", "23Q2", False],
            ["23v2.0.1", "23v2", True],
            ["23Q1.1", "23Q1", True],
            ["2023-01-01", "2023-08-01", False],
            ["23Q2", "2023-01-01", True],
            ["25exec", "25prelim", True],
            ["24exec", "25exec", False],
        ]:
            self.assertEqual(bool_expected, versions.is_newer(version_1, version_2))

    def test_is_newer_invalid_versions(self):
        with self.assertRaises(ValueError):
            versions.is_newer("23v2", "23Q2")
        with self.assertRaises(ValueError):
            versions.is_newer("", "2023-08-01")
        with self.assertRaises(ValueError):
            versions.is_newer("25adopt", "2025-08-01")

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
            ["patch", 1, "23v2", "23v2.0.1"],
            ["patch", 1, "23v2.1", "23v2.1.1"],
            ["patch", 1, "2023-01", "2023-01.1"],
            ["patch", 1, "2023-01.2", "2023-01.3"],
            ["patch", 1, "2023-01-01", "2023-01-01.1"],
            ["patch", 2, "23Q2.1", "23Q2.3"],
            ["major", None, "23v2.0.1", "23v3"],
            ["minor", None, "23v2.2.1", "23v2.3"],
            [None, 2, "23Q4.1", "24Q2"],
            [None, 1, "24exec", "24adopt"],
            [None, 2, "24prelim", "24adopt"],
            ["patch", 1, "24prelim", "24prelim.1"],
            ["patch", 2, "24prelim.2", "24prelim.4"],
        ]:
            self.assertEqual(v_expected, versions.bump(v, bumped_part, bump_by).label)

    def test_group_versions_by_base(self):
        for version, versions_list, expected_output in [
            [
                "24v3",
                ["24v3.0.2", "24v3", "24v3.0.1", "24v3.1", "24Q1"],
                ["24v3", "24v3.0.1", "24v3.0.2"],
            ],
            ["24v4", ["24v3", "24v3.0.1", "24v3.1", "24Q1", "24v4"], ["24v4"]],
            ["24v3", ["23v2"], []],
            ["24prelim", ["24adopt", "24prelim.1", "25prelim"], ["24prelim.1"]],
        ]:
            self.assertEqual(
                expected_output, versions.group_versions_by_base(version, versions_list)
            )

    def test_parse_draft_version_valid_versions(self):
        for draft_version, expected_draft_num, expected_draft_summary in [
            ["1-start", 1, "start"],
            ["1", 1, ""],
            ["123", 123, ""],
            ["2-fix-something", 2, "fix-something"],
            ["3-Not Sure What This Fix Is", 3, "Not Sure What This Fix Is"],
        ]:
            self.assertEqual(
                versions.parse_draft_version(draft_version).revision_num,
                expected_draft_num,
            )
            self.assertEqual(
                versions.parse_draft_version(draft_version).revision_summary,
                expected_draft_summary,
            )

    def test_parse_draft_version_invalid_versions(self):
        with self.assertRaises(ValueError):
            versions.parse_draft_version("InvalidFormat")
        with self.assertRaises(ValueError):
            versions.parse_draft_version("1.something")
        with self.assertRaises(ValueError):
            versions.parse_draft_version("-2-something")
        with self.assertRaises(ValueError):
            versions.parse_draft_version(
                "3-this-draft-version-format-cannot-be-longer-than-defined-max-length"
            )
