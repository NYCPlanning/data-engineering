from unittest import TestCase

from dcpy.utils import versions


class TestVersions(TestCase):
    def test_parsing_valid_versions(self):
        for version, parsed in [
            ["23v2", versions.MajorMinor(year=23, major=2)],
            ["23v2.11", versions.MajorMinor(year=23, major=2, minor=11)],
            ["24v34.56", versions.MajorMinor(year=24, major=34, minor=56)],
            ["2023-01-20", versions.Today(year=2023, month=1, day=20)],
            ["2023-01-01", versions.FirstOfMonth(year=2023, month=1)],
            ["23Q4", versions.Quarter(year=23, quarter=4)],
        ]:
            self.assertEqual(parsed, versions.parse(version))

    def test_parsing_invalid_version(self):
        with self.assertRaises(ValueError):
            versions.parse("23v")
        with self.assertRaises(ValueError):
            versions.parse("2v12")
        with self.assertRaises(ValueError):
            versions.parse("20231212")

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
            [None, 7, "2023-01-02", "2023-01-09"],
            [None, 2, "2023-01-31", "2023-02-02"],
            [None, 1, "2023-04-30", "2023-04-31"],  # there are only 30 days in April!
            [None, 1, "2023-12-01", "2024-01-01"],
        ]:
            self.assertEqual(
                v_expected,
                versions.bump(
                    previous_version=v,
                    bump_type=bumped_part,
                    bump_by=bump_by,
                ),
            )
