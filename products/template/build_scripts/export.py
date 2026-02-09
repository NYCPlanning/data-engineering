import shutil

from dcpy.connectors.edm import publishing
from dcpy.lifecycle import product_metadata
from dcpy.lifecycle.package import pdf_writer, xlsx_writer, yaml_writer
from dcpy.utils.logging import logger

from . import BUILD_KEY, OUTPUT_DIR, PG_CLIENT, PRODUCT_PATH

METADATA_FILES = [
    "source_data_versions.csv",
    "build_metadata.json",
    "data_dictionary.yml",
    "data_dictionary.pdf",
    "data_dictionary.xlsx",
]
BUILD_TABLES = {
    "templatedb": [
        "csv",
        "shapefile_points",
        "shapefile_polygons",
    ],
}


def generate_data_dictionaries():
    org_metadata = product_metadata.load(version="DUMMY VERSION")

    yaml_writer.write_yaml(
        org_metadata=org_metadata,
        product="template_db",
        output_path=PRODUCT_PATH / "data_dictionary.yml",
    )
    pdf_writer.write_pdf(
        org_metadata=org_metadata,
        product="template_db",
        output_path=PRODUCT_PATH / "data_dictionary.pdf",
    )
    xlsx_writer.write_xlsx(
        org_md=org_metadata,
        product="template_db",
        output_path=PRODUCT_PATH / "data_dictionary.xlsx",
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


if __name__ == "__main__":
    generate_data_dictionaries()
    export()
    publishing.upload_build(
        OUTPUT_DIR, BUILD_KEY.product, acl="public-read", build=BUILD_KEY.build
    )
