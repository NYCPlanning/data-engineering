from unittest import TestCase

from dcpy.utils import versions


class TestVersions(TestCase):
    def test_parsing_valid_versions(self):
        for version, parsed in [
            ["23v2", versions.MajorMinor(year=23, major=2)],
            ["23v2.11", versions.MajorMinor(year=23, major=2, minor=11)],
            ["24v34.56", versions.MajorMinor(year=24, major=34, minor=56)],
            ["2020-03-28", versions.Date(year=2020, month=3, day=28)],
            ["2023-01-01", versions.FirstOfMonth(year=2023, month=1)],
            ["23Q4", versions.Quarter(year=23, quarter=4)],
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
                    versions.MajorMinor(year=23, major=2),
                    versions.MajorMinor(year=23, major=1),
                ],
                [
                    versions.MajorMinor(year=23, major=1),
                    versions.MajorMinor(year=23, major=2),
                ],
            ],
            [
                [
                    versions.Date(year=23, month=1, day=2),
                    versions.FirstOfMonth(year=23, month=1),
                ],
                [
                    versions.FirstOfMonth(year=23, month=1),
                    versions.Date(year=23, month=1, day=2),
                ],
            ],
        ]:
            self.assertEqual(sorted, versions.sort(version_list))

    def test_sort_invalid_versions(self):
        with self.assertRaises(TypeError):
            versions.sort(
                [
                    versions.FirstOfMonth(year=23, month=1),
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
            [None, 7, "2023-01-01", "2023-08-01"],
            [None, 1, "2023-12-01", "2024-01-01"],
        ]:
            self.assertEqual(v_expected, versions.bump(v, bumped_part, bump_by))
