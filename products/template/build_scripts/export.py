import os
import shutil
import zipfile
from dcpy.connectors.edm import publishing
from dcpy.utils import geospatial
from dcpy.utils.logging import logger

from . import PRODUCT_S3_NAME, BUILD_NAME, OUTPUT_DIR, PG_CLIENT

METADATA_FILES = [
    "source_data_versions.csv",
    "build_metadata.json",
]
BUILD_TABLES = {
    "templatedb": [
        "csv",
        "shapefile_points",
        "shapefile_polygons",
    ],
}


def export():
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True)
    # export metadata files
    for filename in METADATA_FILES:
        shutil.copy(OUTPUT_DIR.parent / filename, OUTPUT_DIR / filename)

    # export builds tables
    for table_name, file_types in BUILD_TABLES.items():
        data = PG_CLIENT.get_table(table_name)
        file_path = OUTPUT_DIR / table_name
        for file_type in file_types:
            logger.info(
                f"Exporting table\n\t{table_name}\n\tas a {file_type} to\n\t{OUTPUT_DIR}"
            )
            if file_type == "csv":
                data.to_csv(file_path.with_suffix(".csv"), index=False)
            elif "shapefile" in file_type:
                shapefile_directory = OUTPUT_DIR
                geodata = geospatial.convert_to_geodata(
                    data,
                    geometry_column="wkb_geometry",
                    geometry_format=geospatial.GeometryFormat.wkb,
                    crs=geospatial.GeometryCRS.wgs_84_deg,
                )

                if file_type == "shapefile_points":
                    shapefile_directory = OUTPUT_DIR / f"{file_path}_points.shp"
                    geodata = geodata.loc[geodata.geom_type == "Point"]
                elif file_type == "shapefile_polygons":
                    shapefile_directory = OUTPUT_DIR / f"{file_path}_polygons.shp"
                    geodata = geodata.loc[geodata.geom_type == "MultiPolygon"]
                else:
                    raise NotImplementedError(
                        f"Cannot export a shapefile as file type {file_type}"
                    )
                shapefile_directory.mkdir(parents=True)
                geodata.to_file(shapefile_directory)

                logger.info(f"Zipping shapefile\n\t{shapefile_directory}")
                shutil.make_archive(
                    base_name=shapefile_directory,
                    format="zip",
                    root_dir=shapefile_directory,
                )
                shutil.rmtree(shapefile_directory)
            else:
                raise NotImplementedError(
                    f"Cannot export a table as file type {file_type}"
                )


def upload():
    logger.info(
        f"Uploading exported files\n\tfrom {OUTPUT_DIR}\n\tto S3 at {publishing.BUCKET}/{PRODUCT_S3_NAME}/draft/{BUILD_NAME}"
    )
    publishing.upload(
        OUTPUT_DIR,
        publishing.DraftKey(PRODUCT_S3_NAME, BUILD_NAME),
        acl="public-read",
    )


if __name__ == "__main__":
    export()
    upload()
