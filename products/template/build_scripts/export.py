import shutil

from dcpy.lifecycle.package import generate_metadata_assets
from dcpy.connectors.edm import product_metadata, publishing
from dcpy.utils.logging import logger

from . import PRODUCT_PATH, OUTPUT_DIR, PRODUCT_S3_NAME, BUILD_NAME, PG_CLIENT

METADATA_FILES = [
    "source_data_versions.csv",
    "build_metadata.json",
    "data_dictionary.pdf",
]
BUILD_TABLES = {
    "templatedb": [
        "csv",
        "shapefile_points",
        "shapefile_polygons",
    ],
}


def generate_pdfs():
    data_dictionary_path = product_metadata.download(
        "template_db", OUTPUT_DIR / "data_dictionary.yaml", dataset="template_db"
    )
    output_html_path = generate_metadata_assets.generate_html_from_yaml(
        data_dictionary_path,
        OUTPUT_DIR / "data_dictionary.html",
        generate_metadata_assets.DEFAULT_DATA_DICTIONARY_TEMPLATE_PATH,
    )
    generate_metadata_assets.generate_pdf_from_html(
        output_html_path, PRODUCT_PATH / "data_dictionary.pdf"
    )


def export():
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True)
    # export metadata files
    for filename in METADATA_FILES:
        shutil.copy(PRODUCT_PATH / filename, OUTPUT_DIR / filename)

    # export builds tables
    for table_name, file_types in BUILD_TABLES.items():
        file_path = OUTPUT_DIR / table_name
        for file_type in file_types:
            logger.info(
                f"Exporting table\n\t{table_name}\n\tas a {file_type} to\n\t{OUTPUT_DIR}"
            )
            if file_type == "csv":
                data = PG_CLIENT.read_table_df(table_name)
                data.to_csv(file_path.with_suffix(".csv"), index=False)
            elif "shapefile" in file_type:
                data = PG_CLIENT.read_table_gdf(table_name, geom_column="wkb_geometry")
                shapefile_directory = OUTPUT_DIR

                if file_type == "shapefile_points":
                    shapefile_directory = OUTPUT_DIR / f"{table_name}_points"
                    data = data.loc[data.geom_type == "Point"]
                elif file_type == "shapefile_polygons":
                    shapefile_directory = OUTPUT_DIR / f"{table_name}_polygons"
                    data = data.loc[data.geom_type == "MultiPolygon"]
                else:
                    raise NotImplementedError(
                        f"Cannot export a shapefile as file type {file_type}"
                    )
                data.to_file(shapefile_directory)

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
        f"Uploading exported files\n\tfrom {OUTPUT_DIR}\n\tto S3 at {publishing.BUCKET}/{PRODUCT_S3_NAME}/build/{BUILD_NAME}"
    )
    publishing.upload(
        OUTPUT_DIR,
        publishing.BuildKey(PRODUCT_S3_NAME, BUILD_NAME),
        acl="public-read",
    )


if __name__ == "__main__":
    generate_pdfs()
    export()
    upload()
