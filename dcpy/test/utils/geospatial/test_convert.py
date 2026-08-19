import json

import pytest

from dcpy.utils.geospatial import convert

RECORDS = [
    {
        "locationID": "1",
        "latitude": "40.7",
        "longitude": "-73.9",
        "serviceHours": [{"day": "MO", "times": []}],
        "parcelLockerCity": None,
    },
    {
        "locationID": "2",
        "latitude": "40.8",
        "longitude": "-74.0",
        "serviceHours": None,
        "parcelLockerCity": "NEW YORK",
    },
]


class TestShapefileFieldNames:
    def test_short_names_are_left_alone(self):
        assert convert.shapefile_field_names(["zip5", "state"]) == {
            "zip5": "zip5",
            "state": "state",
        }

    def test_names_sharing_a_prefix_stay_distinct(self):
        """Plain truncation maps all five parcelLocker* fields onto `parcelLock`."""
        columns = [
            "parcelLockerAddress",
            "parcelLockerCity",
            "parcelLockerLocation",
            "parcelLockerState",
            "parcelLockerZip",
        ]
        names = convert.shapefile_field_names(columns)
        assert len(set(names.values())) == len(columns)
        assert all(len(n) <= convert.SHAPEFILE_NAME_LIMIT for n in names.values())

    def test_every_name_fits_the_format_limit(self):
        long = "passportTelephoneNumberExtension"
        assert len(convert.shapefile_field_names([long])[long]) <= 10


class TestReadPoints:
    def test_builds_points_from_string_coordinates(self, tmp_path):
        path = tmp_path / "in.json"
        path.write_text(json.dumps(RECORDS))
        gdf = convert.read_points(path)
        assert len(gdf) == 2
        assert set(gdf.geom_type) == {"Point"}
        assert gdf.crs == convert.WGS84

    def test_unparseable_coordinates_raise(self, tmp_path):
        """A silent coerce would place the record at (0, 0), off West Africa."""
        path = tmp_path / "in.json"
        path.write_text(json.dumps([{**RECORDS[0], "latitude": "n/a"}]))
        with pytest.raises(ValueError, match="unparseable coordinates"):
            convert.read_points(path)

    def test_missing_coordinate_column_raises(self, tmp_path):
        path = tmp_path / "in.json"
        path.write_text(json.dumps([{"locationID": "1"}]))
        with pytest.raises(ValueError, match="latitude"):
            convert.read_points(path)


class TestWriteGeojson:
    def test_properties_survive_intact(self, tmp_path):
        """GDAL's writer renders nested values via repr, which isn't valid JSON."""
        source = tmp_path / "in.json"
        source.write_text(json.dumps(RECORDS))
        out = convert.write_geojson(
            convert.read_points(source), tmp_path / "out.geojson"
        )

        features = json.loads(out.read_text())["features"]
        assert [f["properties"] for f in features] == RECORDS
        assert isinstance(features[0]["properties"]["serviceHours"], list)


class TestWriteShapefile:
    def test_writes_a_zip_with_the_field_mapping(self, tmp_path):
        import zipfile

        source = tmp_path / "in.json"
        source.write_text(json.dumps(RECORDS))
        convert.write_shapefile(convert.read_points(source), tmp_path / "out.shp")

        names = set(zipfile.ZipFile(tmp_path / "out_shapefile.zip").namelist())
        assert {"out.shp", "out.dbf", "out.prj", "out_fields.csv"} <= names
