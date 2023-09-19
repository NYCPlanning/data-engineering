from unittest import TestCase

from dcpy.utils import versions


class TestVersions(TestCase):
    def test_parsing_valid_versions(self):
        for version, parsed in [
            ["23v2", {"year": "23", "major": "2"}],
            ["23v2.11", {"year": "23", "major": "2", "minor": "11"}],
            ["24v34.56", {"year": "24", "major": "34", "minor": "56"}],
        ]:
            self.assertEqual(parsed, versions.parse(version))

    def test_parsing_invalid_version(self):
        with self.assertRaises(Exception):
            versions.parse("23v")
        with self.assertRaises(Exception):
            versions.parse("2v12")

    def test_bumping_versions(self):
        for bumped_part, v, v_expected in [
            ["major", "23v2", "23v3"],
            ["major", "23v2.1", "23v3"],
            ["minor", "23v2", "23v2.1"],
            ["minor", "23v2.1", "23v2.2"],
        ]:
            self.assertEqual(v_expected, versions.bump(v, bumped_part))
