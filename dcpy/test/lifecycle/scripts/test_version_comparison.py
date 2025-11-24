import pytest
from dcpy.lifecycle.scripts.version_compare import FuzzyVersion


class TestFuzzyVersionNormalization:
    @pytest.mark.parametrize(
        "input_version,expected",
        [
            ("september 2025", "202509"),
            ("SEPTEMBER 2025", "202509"),
            ("march 2024", "202403"),
            ("December 2023", "202312"),
        ],
    )
    def test_month_name_year_format(self, input_version, expected):
        """Test normalization of month name + year format."""
        fv = FuzzyVersion(input_version)
        assert fv.normalized == expected

    @pytest.mark.parametrize(
        "input_version,expected",
        [
            ("25q1", "202503"),
            ("24q2", "202406"),
            ("23q3", "202309"),
            ("22q4", "202212"),
            ("25Q2", "202506"),  # Case insensitive
        ],
    )
    def test_quarter_notation(self, input_version, expected):
        """Test normalization of quarter notation."""
        fv = FuzzyVersion(input_version)
        assert fv.normalized == expected

    @pytest.mark.parametrize(
        "input_version,expected",
        [
            ("20250925", "202509"),
            ("20240315", "202403"),
            ("20231201", "202312"),
        ],
    )
    def test_yyyymmdd_format(self, input_version, expected):
        """Test normalization of YYYYMMDD format."""
        fv = FuzzyVersion(input_version)
        assert fv.normalized == expected

    @pytest.mark.parametrize(
        "input_version,expected",
        [
            ("202509", "202509"),
            ("202403", "202403"),
            ("202312", "202312"),
        ],
    )
    def test_yyyymm_format(self, input_version, expected):
        """Test YYYYMM format (should remain unchanged)."""
        fv = FuzzyVersion(input_version)
        assert fv.normalized == expected

    @pytest.mark.parametrize(
        "input_version,expected",
        [
            ("25v3", "25v3"),  # No date pattern, should remain unchanged
            ("v2025", "v2025"),
            ("release-1.0", "release-1.0"),
        ],
    )
    def test_version_notation(self, input_version, expected):
        """Test version notation that doesn't match date patterns."""
        fv = FuzzyVersion(input_version)
        assert fv.normalized == expected

    @pytest.mark.parametrize(
        "input_version,expected",
        [
            ("", ""),
            ("invalid", "invalid"),
            ("2025", "2025"),  # Year alone, no month
            ("13", "13"),  # Invalid month
        ],
    )
    def test_edge_cases(self, input_version, expected):
        """Test edge cases and invalid inputs."""
        fv = FuzzyVersion(input_version)
        assert fv.normalized == expected

    @pytest.mark.parametrize(
        "input_version,expected",
        [
            ("  september 2025  ", "202509"),
            ("\tmarch 2024\n", "202403"),
            ("  25q2  ", "202506"),
        ],
    )
    def test_whitespace_handling(self, input_version, expected):
        """Test that whitespace is properly handled."""
        fv = FuzzyVersion(input_version)
        assert fv.normalized == expected


class TestFuzzyVersionComparison:
    @pytest.mark.parametrize(
        "version1,version2",
        [
            ("202509", "202509"),
            ("september 2025", "september 2025"),
            ("25q2", "25q2"),
        ],
    )
    def test_exact_matches(self, version1, version2):
        """Test that identical versions match."""
        fv1 = FuzzyVersion(version1)
        fv2 = FuzzyVersion(version2)
        assert fv1.probably_equals(fv2) is True

    @pytest.mark.parametrize(
        "version1,version2",
        [
            ("25v3", "25V3"),
            ("September 2025", "SEPTEMBER 2025"),
            ("q2", "Q2"),
        ],
    )
    def test_case_differences(self, version1, version2):
        """Test that case differences are handled correctly."""
        fv1 = FuzzyVersion(version1)
        fv2 = FuzzyVersion(version2)
        assert fv1.probably_equals(fv2) is True

    @pytest.mark.parametrize(
        "version1,version2,expected",
        [
            ("september 2025", "202509", True),
            ("march 2024", "202403", True),
            ("december 2023", "202312", True),
            ("september 2025", "202508", False),  # Wrong month
        ],
    )
    def test_month_name_vs_numeric(self, version1, version2, expected):
        """Test month name vs numeric format comparison."""
        fv1 = FuzzyVersion(version1)
        fv2 = FuzzyVersion(version2)
        assert fv1.probably_equals(fv2) is expected

    @pytest.mark.parametrize(
        "version1,version2,expected",
        [
            ("june 2025", "25q2", True),
            ("march 2025", "25q1", True),
            ("september 2025", "25q3", True),
            ("december 2025", "25q4", True),
            ("june 2025", "25q1", False),  # Wrong quarter
        ],
    )
    def test_quarter_comparisons(self, version1, version2, expected):
        """Test quarter notation comparisons."""
        fv1 = FuzzyVersion(version1)
        fv2 = FuzzyVersion(version2)
        assert fv1.probably_equals(fv2) is expected

    @pytest.mark.parametrize(
        "version1,version2,expected",
        [
            ("september 2025", "20250925", True),
            ("march 2024", "20240315", True),
            ("september 2025", "20250825", False),  # Wrong month
        ],
    )
    def test_date_format_variations(self, version1, version2, expected):
        """Test various date format comparisons."""
        fv1 = FuzzyVersion(version1)
        fv2 = FuzzyVersion(version2)
        assert fv1.probably_equals(fv2) is expected

    @pytest.mark.parametrize(
        "version1,version2",
        [
            ("september 2025", "october 2025"),
            ("202509", "202510"),
            ("25q2", "25q3"),
            ("march 2024", "june 2024"),
        ],
    )
    def test_non_matching_dates(self, version1, version2):
        """Test that different dates don't match."""
        fv1 = FuzzyVersion(version1)
        fv2 = FuzzyVersion(version2)
        assert fv1.probably_equals(fv2) is False

    @pytest.mark.parametrize(
        "version1,version2,expected",
        [
            ("", "", False),  # Both empty should not match (no version info)
            ("", "202509", False),
            ("202509", "", False),
            (None, "202509", False),
            ("202509", None, False),
        ],
    )
    def test_empty_and_none_inputs(self, version1, version2, expected):
        """Test handling of empty and None inputs."""
        fv1 = FuzzyVersion(version1)
        fv2 = FuzzyVersion(version2)
        assert fv1.probably_equals(fv2) is expected

    @pytest.mark.parametrize(
        "version1,version2,expected",
        [
            ("25v3", "25V3", True),  # Case difference
            ("v1.0", "V1.0", True),  # Case difference in non-date
        ],
    )
    def test_non_date_versions(self, version1, version2, expected):
        """Test handling of non-date version strings."""
        fv1 = FuzzyVersion(version1)
        fv2 = FuzzyVersion(version2)
        assert fv1.probably_equals(fv2) is expected

    @pytest.mark.parametrize(
        "version1,version2,expected",
        [
            ("september 2025", "202509", True),
            ("25v3", "25V3", True),
            ("june 2024", "24q2", True),
            ("different", "versions", False),
        ],
    )
    def test_mixed_formats(self, version1, version2, expected):
        """Test various mixed format combinations."""
        fv1 = FuzzyVersion(version1)
        fv2 = FuzzyVersion(version2)
        assert fv1.probably_equals(fv2) is expected

    @pytest.mark.parametrize(
        "version1,version2",
        [
            ("  september 2025  ", "202509"),
            ("march 2024\n", "  202403"),
            ("\t25q2  ", "202506\t"),
        ],
    )
    def test_whitespace_robustness(self, version1, version2):
        """Test that whitespace doesn't affect comparison."""
        fv1 = FuzzyVersion(version1)
        fv2 = FuzzyVersion(version2)
        assert fv1.probably_equals(fv2)


class TestIntegrationScenarios:
    @pytest.mark.parametrize(
        "version1,version2,expected",
        [
            ("September 2025", "202509", True),
            ("JUNE 2024", "24q2", True),
            ("march 2025", "20250315", True),
            ("Q1 2025", "january 2025", False),  # Different months in Q1
            ("25v3", "25V3", True),
            ("25q3", "25Q3", True),
        ],
    )
    def test_real_world_version_comparisons(self, version1, version2, expected):
        """Test realistic version comparison scenarios."""
        fv1 = FuzzyVersion(version1)
        fv2 = FuzzyVersion(version2)
        assert fv1.probably_equals(fv2) is expected
