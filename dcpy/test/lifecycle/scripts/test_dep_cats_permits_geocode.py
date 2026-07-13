from unittest.mock import patch

import pandas as pd
import pytest

from dcpy.lifecycle.scripts import dep_cats_permits_geocode as m


class TestCleanBoroName:
    @pytest.mark.parametrize(
        "input_value,expected",
        [
            ("STATENISLAND", "Staten Island"),
            ("BRONX", "Bronx"),
            ("MANHATTAN", "Manhattan"),
            ("NOT A BOROUGH", None),
            (None, None),
        ],
    )
    def test_clean_boro_name(self, input_value, expected):
        assert m.clean_boro_name(input_value) == expected


class TestCleanHouse:
    @pytest.mark.parametrize(
        "input_value,expected",
        [
            ("142", "142"),
            ("142 (rear)", "142"),
            ("142/144", "142"),
            (None, ""),
            (float("nan"), ""),
            (pd.NA, ""),
        ],
    )
    def test_clean_house(self, input_value, expected):
        assert m.clean_house(input_value) == expected


class TestCleanStreet:
    @pytest.mark.parametrize(
        "input_value,expected",
        [
            ("FULTON STREET", "FULTON STREET"),
            ("O'BRIEN AVE", "OBRIEN AVE"),
            ("VARIOUS LOCATIONS", " "),
            ("JFK AIRPORT ACCESS RD", "JFK INTERNATIONAL AIRPORT"),
            (None, ""),
            (float("nan"), ""),
            (pd.NA, ""),
            ("48TH\xa0STREET", "48TH STREET"),  # non-breaking space
            ("AVENUE OF THE AMÉRICAS", "AVENUE OF THE AMERICAS"),
        ],
    )
    def test_clean_street(self, input_value, expected):
        assert m.clean_street(input_value) == expected
        assert m.clean_street(input_value).isascii()


class TestMissingAddressFieldsRegression:
    """Regression coverage for a bug caught in production: pandas' newer Arrow-backed
    string dtype (default under `future.infer_string`, pandas 3.x) doesn't reliably
    turn NaN into the literal string "nan" via `.astype(str)`, leaving an actual float
    `nan` that crashed clean_house's regex. See CI run 29427567768/job/87393861307.
    """

    def test_clean_house_survives_future_infer_string_series(self):
        with pd.option_context("future.infer_string", True):
            s = pd.Series([142.0, float("nan"), 88.0], dtype="float64")
            out = s.apply(m.clean_house)
        assert list(out) == ["142.0", "", "88.0"]

    def test_process_handles_missing_house_number(self):
        # A permit geocoded via intersection (no house number at all) alongside a
        # normal house+street permit, under the same dtype conditions as CI.
        with pd.option_context("future.infer_string", True):
            df = pd.DataFrame(
                [
                    _base_row(house=None, street="77 STREET & CENTRAL PARK WEST"),
                    _base_row(applicationid="PB000002"),
                ]
            )
            with patch.object(m, "geocode", side_effect=_fake_geocode()):
                out = m.process(df)
        assert len(out) == 2


class TestNonAsciiStreetNameRegression:
    """Regression coverage for a second bug caught in production: a street name with
    a non-breaking space (U+00A0) crashed deep inside the `geosupport` package with
    `ValueError: memoryview assignment: lvalue and rvalue have different structures`.
    Geosupport's fixed-width buffer expects one byte per character; a non-ASCII
    character UTF-8-encodes to 2+ bytes and overflows the slot. See CI run
    29429205138/job/87399516981. Confirmed against live data: 2 real dep_cats_permits
    rows have this exact non-breaking space in their street name.
    """

    def test_hnum_and_sname_are_always_ascii(self):
        df = _base_df(house="142", street="48TH\xa0STREET")
        with patch.object(m, "geocode", side_effect=_fake_geocode()):
            out = m.process(df)
        assert out.loc[0, "sname"] == "48TH STREET"
        assert out.loc[0, "sname"].isascii()
        assert out.loc[0, "hnum"].isascii()


class TestFindStretch:
    def test_finds_stretch(self):
        assert m.find_stretch("MAIN ST BETWEEN 1ST AVE AND 2ND AVE") == (
            "MAIN ST",
            "1ST AVE",
            "2ND AVE",
        )

    def test_no_stretch(self):
        assert m.find_stretch("142 FULTON STREET") == ("", "", "")


class TestFindIntersection:
    @pytest.mark.parametrize(
        "input_value,expected",
        [
            ("77 STREET & CENTRAL PARK WEST", ("77 STREET", "CENTRAL PARK WEST")),
            ("MAIN ST AND BROADWAY", ("MAIN ST", "BROADWAY")),
            ("142 FULTON STREET", ("", "")),
        ],
    )
    def test_find_intersection(self, input_value, expected):
        assert m.find_intersection(input_value) == expected


def _fake_geocode(fn: str = "1B"):
    def _geocode(inputs: dict) -> dict:
        return dict(
            geo_housenum="",
            geo_streetname="",
            geo_address=None,
            geo_bbl="1000010001",
            geo_bin="1000000",
            geo_latitude="40.7",
            geo_longitude="-74.0",
            geo_x_coord="980000",
            geo_y_coord="200000",
            geo_grc="00",
            geo_function=fn,
        )

    return _geocode


def _base_row(**overrides) -> dict:
    row = dict(
        requestid="1",
        applicationid="PB000001",
        requesttype="NEW BOILER REGISTRATION",
        house="142",
        street="FULTON STREET",
        borough="MANHATTAN",
        bin="1000000",
        block="1",
        lot="1",
        ownername="Owner",
        expirationdate="01/01/2030",
        make="Make",
        model="Model",
        burnermake="Burner",
        burnermodel="BurnerModel",
        primaryfuel="Gas",
        secondaryfuel="",
        quantity="1",
        issuedate="01/01/2020",
        status="CURRENT",
        premisename="Premise",
    )
    row.update(overrides)
    return row


def _base_df(**overrides) -> pd.DataFrame:
    return pd.DataFrame([_base_row(**overrides)])


class TestProcessFiltering:
    def test_excludes_cancelled(self):
        df = _base_df(status="CANCELLED")
        with patch.object(m, "geocode", side_effect=_fake_geocode()):
            assert len(m.process(df)) == 0

    def test_excludes_g_prefix(self):
        df = _base_df(applicationid="G000001")
        with patch.object(m, "geocode", side_effect=_fake_geocode()):
            assert len(m.process(df)) == 0

    def test_excludes_c_registration(self):
        df = _base_df(applicationid="C000001", requesttype="REGISTRATION")
        with patch.object(m, "geocode", side_effect=_fake_geocode()):
            assert len(m.process(df)) == 0

    def test_keeps_c_non_registration(self):
        df = _base_df(applicationid="C000001", requesttype="WORK PERMIT")
        with patch.object(m, "geocode", side_effect=_fake_geocode()):
            assert len(m.process(df)) == 1

    def test_excludes_ca_expired_work_permit(self):
        df = _base_df(
            applicationid="CA000001", requesttype="WORK PERMIT", status="EXPIRED"
        )
        with patch.object(m, "geocode", side_effect=_fake_geocode()):
            assert len(m.process(df)) == 0

    def test_keeps_ca_expired_non_work_permit(self):
        # requesttype must be outside the registration set too, so this isolates the
        # CA/WORK PERMIT/EXPIRED rule from the separate C-prefix/registration rule
        # ("CA..." also starts with "C").
        df = _base_df(
            applicationid="CA000001", requesttype="INSPECTION", status="EXPIRED"
        )
        with patch.object(m, "geocode", side_effect=_fake_geocode()):
            assert len(m.process(df)) == 1

    def test_excludes_grc_71_rejection(self):
        df = _base_df()

        def geocode_rejected(inputs: dict) -> dict:
            geo = _fake_geocode()(inputs)
            geo["geo_grc"] = "71"
            return geo

        with patch.object(m, "geocode", side_effect=geocode_rejected):
            assert len(m.process(df)) == 0

    def test_excludes_rows_missing_final_geometry(self):
        df = _base_df()

        def geocode_no_coords(inputs: dict) -> dict:
            geo = _fake_geocode()(inputs)
            geo["geo_latitude"] = ""
            geo["geo_longitude"] = ""
            return geo

        with patch.object(m, "geocode", side_effect=geocode_no_coords):
            assert len(m.process(df)) == 0


class TestProcessGeometry:
    def test_non_intersection_uses_lon_lat_directly(self):
        df = _base_df()
        with patch.object(m, "geocode", side_effect=_fake_geocode("1B")):
            out = m.process(df)
        assert out.loc[0, "longitude"] == pytest.approx(-74.0)
        assert out.loc[0, "latitude"] == pytest.approx(40.7)

    def test_intersection_reprojects_from_state_plane(self):
        df = _base_df(house="", street="77 STREET & CENTRAL PARK WEST")
        with patch.object(m, "geocode", side_effect=_fake_geocode("Intersection")):
            out = m.process(df)
        # Reprojected from EPSG:2263 (980000, 200000), not the raw fake lon/lat (-74.0, 40.7)
        assert out.loc[0, "longitude"] != pytest.approx(-74.0)
        assert -74.1 < out.loc[0, "longitude"] < -73.9
        assert 40.6 < out.loc[0, "latitude"] < 40.8

    def test_jfk_defaults_to_queens_when_borough_missing(self):
        df = _base_df(borough=None, street="JFK AIRPORT ACCESS RD")
        with patch.object(m, "geocode", side_effect=_fake_geocode()):
            out = m.process(df)
        assert out.loc[0, "borough"] == "Queens"
