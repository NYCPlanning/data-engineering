from __future__ import annotations
from typing import NotRequired, TypedDict

S_MEDIA_FILES_URL_BASE = "https://s-media.nyc.gov/agencies/dcp/assets/files/"
PLANNING_ASSETS_URL_BASE = "https://www1.nyc.gov/assets/planning/download/"

FILE_TYPE_SUFFIXES_TO_URL_PART = {
    "xlsx": "excel/data-tools/bytes/",
    "pdf": "pdf/data-tools/bytes/",
    "zip": "zip/data-tools/bytes/",
}

BYTES_CATALOG_URL_PREFIX = (
    "https://www.nyc.gov/assets/planning/json/content/resources/dataset-archives"
)
BYTES_API_PREFIX = (
    "https://apps.nyc.gov/content-api/v1/content/planning/resources/datasets"
)


def get_product_dataset_bytes_resource(product, dataset) -> str:
    """The Bytes convention for datasets seems to be that for each, there's a resource name like 'atomic-polygons' below:
            https://www.nyc.gov/content/planning/pages/resources/datasets/atomic-polygons
            https://apps.nyc.gov/content-api/v1/content/planning/resources/datasets/atomic-polygons
            https://www.nyc.gov/assets/planning/json/content/resources/dataset-archives/atomic-polygons.json
    For a dataset like lion.atomic_polygons, their dataset name matches up with ours (after twiddling the dash and underscore)
    but for a dataset like pluto, we're not so lucky so we need to do some overriding.
    """
    return SITE_MAP[product][dataset].get("bytes_dataset_key") or dataset.replace(
        "_", "-"
    )


def get_most_recent_version_url(product, dataset) -> str:
    return f"{BYTES_API_PREFIX}/{get_product_dataset_bytes_resource(product, dataset)}"


def get_dataset_catalog_json_url(product, dataset) -> str | None:
    conf = SITE_MAP[product][dataset]
    if conf.get("no_archived_versions"):
        return None

    json_file_start = conf.get(
        "catalog_file_override"
    ) or get_product_dataset_bytes_resource(product, dataset)
    return f"{BYTES_CATALOG_URL_PREFIX}/{json_file_start}.json"


def all_product_dataset_files() -> list[tuple[str, str, str]]:
    """Get a list of all (product, dataset, files)"""
    files = []
    for prod, ds in SITE_MAP.items():
        for ds, ds_conf in ds.items():  # type: ignore
            for f_id in ds_conf["files"]:
                files.append((prod, ds, f_id))
    return files  # type: ignore


def all_product_datasets() -> list[tuple[str, str]]:
    """Get a list of all (product, dataset)"""
    pds = []
    for prod, ds in SITE_MAP.items():
        for ds, ds_conf in ds.items():  # type: ignore
            pds.append((prod, ds))
    return pds  # type: ignore


def get_filename(product, dataset, file_id, *, version="") -> str:
    """Returns a function to resolve a (potentially templated) filename."""
    f = SITE_MAP[product][dataset]["files"][file_id]
    if "filename_template" in f:
        assert version, (
            "Resolving a url for a templated file. We need a version passed!"
        )
        return f["filename_template"](version)
    else:
        return f["filename"]


def get_file_url(product, dataset, file_id, *, version="") -> str:
    ds_conf = SITE_MAP[product][dataset]

    file_res_name: str = ds_conf.get(
        "file_resource_override"
    ) or get_product_dataset_bytes_resource(product, dataset)

    assert file_id in ds_conf["files"], (
        f"No file_id `{file_id}` is defined in the site_map for {product}.{dataset}"
    )
    f = ds_conf["files"][file_id]
    base_url = f.get("url_base_override", S_MEDIA_FILES_URL_BASE)

    filename = get_filename(product, dataset, file_id, version=version)
    suffix = filename.rsplit(".", 1)[-1]

    # Calculate the middle part of the url, e.g. zip/data-tools/bytes/lion-differences-file
    if suffix == "zip":
        url_mid_part = FILE_TYPE_SUFFIXES_TO_URL_PART[suffix] + (
            file_res_name + "/" if file_res_name else ""
        )
    else:
        url_mid_part = FILE_TYPE_SUFFIXES_TO_URL_PART[suffix]

    return f"{base_url}{url_mid_part}{filename}"


def all_urls(version) -> dict:
    return {
        pdf: get_file_url(*pdf, version=version) for pdf in all_product_dataset_files()
    }


class DatasetConfig(TypedDict):
    files: dict[str, dict]

    # Some pages don't have archived versions
    no_archived_versions: NotRequired[bool]

    # The closest thing to an actual dataset key
    # in certain cases (e.g. COLP) we get lucky and all resources
    # e.g. the JSON versions file, the page url etc, make use of this identifier
    bytes_dataset_key: NotRequired[str]

    # For when the JSON catalog doesn't match the bytes_dataset_key
    catalog_file_override: NotRequired[str]
    # For when the file_path doesn't match the bytes_dataset_key
    file_resource_override: NotRequired[str]


SITE_MAP: dict[str, dict[str, DatasetConfig]] = {
    "building_elevation_and_subgrade": {
        "building_elevation_and_subgrade": {
            "no_archived_versions": True,
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"bes_{v}.zip",
                },
                "readme": {
                    "filename": "bes_readme.pdf",
                },
                "data_dictionary": {
                    "filename": "bes_datadictionary.xlsx",
                },
            },
            "bytes_dataset_key": "building-evaluation-subgrade",
        }
    },
    "colp": {
        "colp": {
            "files": {
                "colp_readme.pdf": {
                    "filename": "colp_readme.pdf",
                },
                "colp_metadata.pdf": {
                    "filename": "colp_metadata.pdf",
                },
                "primary_shapefile": {
                    "filename_template": lambda v: f"colp_{v}_shp.zip",
                },
                "csv_zip": {
                    "filename_template": lambda v: f"colp_{v}_csv.zip",
                },
                "xlsx_zip": {
                    "filename_template": lambda v: f"colp_{v}_xlsx.zip",
                },
                "primary_file_geodatabase": {
                    "filename_template": lambda v: f"colp_{v}_gdb.zip",
                },
            },
            "bytes_dataset_key": "city-owned-leased-properties",
        }
    },
    "cdbg": {
        "cdbg": {
            "files": {
                "csv_zip": {
                    "filename_template": lambda v: f"nyc_cdbg_eligible_2020_census_tracts_{v}_csv.zip",
                },
                "fgdb": {
                    "filename_template": lambda v: f"nyc_cdbg_eligible_2020_census_tracts_{v}_gdb.zip",
                },
                "metadata_pdf": {
                    "filename": "cdbg_eligible_2020_census_tracts_metadata.pdf",
                },
            },
            "bytes_dataset_key": "community-development-block-grant",
        }
    },
    "cpdb": {
        "commitments": {
            "files": {
                "commitments_zip": {
                    "filename_template": lambda v: f"cpdb_planned_commitments_{v}.zip",
                }
            },
            "bytes_dataset_key": "capital-projects-database",
        },
        "projects": {
            "files": {
                "csv_package": {
                    "filename_template": lambda v: f"cpdb_{v}.zip",
                },
                "projects_points": {
                    "filename_template": lambda v: f"cpdb_projects_pts_{v}.zip",
                },
                "projects_polygons": {
                    "filename_template": lambda v: f"cpdb_projects_poly_{v}.zip",
                },
            },
            "bytes_dataset_key": "capital-projects-database",
        },
    },
    "dcm": {
        "digital_city_map": {
            "files": {
                "dcm_multilayer": {
                    "filename_template": lambda v: f"dcm_{v}shp.zip",
                },
                "data_dictionary_pdf": {
                    "filename": "dcm.pdf",
                },
            },
            "bytes_dataset_key": "digital-city-map",
        },
        "street_center_line": {
            "files": {
                "dcm_multilayer": {
                    "filename_template": lambda v: f"dcm_{v}shp.zip",
                },
                "data_dictionary_pdf": {
                    "filename": "dcm_street_centerline.pdf",
                },
            },
            "bytes_dataset_key": "digital-city-map",
        },
        "city_map_alterations": {
            "files": {
                "dcm_multilayer": {
                    "filename_template": lambda v: f"dcm_{v}shp.zip",
                },
                "data_dictionary_pdf": {
                    "filename": "dcm_city_map_alterations.pdf",
                },
            },
            "bytes_dataset_key": "digital-city-map",
        },
        "street_name_changes": {
            "files": {
                "dcm_multilayer": {
                    "filename_template": lambda v: f"dcm_{v}shp.zip",
                },
                "areas_data_dictionary_pdf": {
                    "filename": "dcm_street_name_changes_areas.pdf",
                },
                "lines_data_dictionary_pdf": {
                    "filename": "dcm_street_name_changes_lines.pdf",
                },
                "points_data_dictionary_pdf": {
                    "filename": "dcm_street_name_changes_points.pdf",
                },
            },
            "bytes_dataset_key": "digital-city-map",
        },
        "arterials_major_streets": {
            "files": {
                "dcm_multilayer": {
                    "filename_template": lambda v: f"dcm_{v}shp.zip",
                },
                "data_dictionary_pdf": {
                    "filename": "dcm_arterials_major_streets.pdf",
                },
            },
            "bytes_dataset_key": "digital-city-map",
        },
        "digital_city_map__geodatabase": {
            "files": {
                "fgdb": {
                    "filename_template": lambda v: f"dcm_{v}fdgb.zip",
                },
                "dcm_street_name_changes_areas_pdf": {
                    "filename": "dcm_street_name_changes_areas.pdf",
                },
                "dcm_street_name_changes_lines_pdf": {
                    "filename": "dcm_street_name_changes_lines.pdf",
                },
                "dcm_street_name_changes_points_pdf": {
                    "filename": "dcm_street_name_changes_points.pdf",
                },
                "dcm_arterials_major_streets_pdf": {
                    "filename": "dcm_arterials_major_streets.pdf",
                },
                "dcm_city_map_alterations_pdf": {
                    "filename": "dcm_city_map_alterations.pdf",
                },
                "dcm_pdf": {
                    "filename": "dcm.pdf",
                },
                "dcm_street_centerline_pdf": {
                    "filename": "dcm_street_centerline.pdf",
                },
            },
            "bytes_dataset_key": "digital-city-map",
        },
    },
    "developments": {
        "housing_database": {
            "bytes_dataset_key": "housing-project-level",
            "files": {
                "csv": {
                    "filename_template": lambda v: f"nychousingdb_{v}_csv.zip",
                },
                "gdb": {
                    "filename_template": lambda v: f"nychousingdb_{v}_gdb.zip",
                },
                "shapefile": {
                    "filename_template": lambda v: f"nychousingdb_{v}_shp.zip",
                },
                "data_dictionary": {
                    "filename": "Housing_Database_Data_Dictionary.xlsx",
                },
                "data_dictionary_summary_files": {
                    "filename": "Housing_Summary_Files_Data_Dictionary.xlsx",
                },
            },
        },
        "housing_database_project_level_files": {
            "bytes_dataset_key": "housing-project-level",
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nychdb_{v}_shp.zip",
                },
                "csv": {
                    "filename_template": lambda v: f"nychdb_{v}_csv.zip",
                },
                "shapefile_inactive_included": {
                    "filename_template": lambda v: f"nychdb_inactiveincluded_{v}_shp.zip",
                },
                "csv_inactive_included": {
                    "filename_template": lambda v: f"nychdb_inactiveincluded_{v}_csv.zip",
                },
                "data_dictionary": {
                    "filename": "Housing_Database_Data_Dictionary.xlsx",
                },
            },
        },
        "housing_database_by_2020_census_block": {
            "bytes_dataset_key": "housing-unit-change-summary",
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nychdb_cblock_{v}_shp.zip",
                },
                "csv": {
                    "filename_template": lambda v: f"nychdb_cblock_{v}_csv.zip",
                },
                "data_dictionary": {
                    "filename": "Housing_Summary_Files_Data_Dictionary.xlsx",
                },
            },
        },
        "housing_database_by_2020_census_tract": {
            "bytes_dataset_key": "housing-unit-change-summary",
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nychdb_ctract_{v}_shp.zip",
                },
                "csv": {
                    "filename_template": lambda v: f"nychdb_ctract_{v}_csv.zip",
                },
                "data_dictionary": {
                    "filename": "Housing_Summary_Files_Data_Dictionary.xlsx",
                },
            },
        },
        "housing_database_by_2020_nta": {
            "bytes_dataset_key": "housing-unit-change-summary",
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nychdb_nta_{v}_shp.zip",
                },
                "csv": {
                    "filename_template": lambda v: f"nychdb_nta_{v}_csv.zip",
                },
                "data_dictionary": {
                    "filename": "Housing_Summary_Files_Data_Dictionary.xlsx",
                },
            },
        },
        "housing_database_by_2020_cdta": {
            "bytes_dataset_key": "housing-unit-change-summary",
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nychdb_cdta_{v}_shp.zip",
                },
                "csv": {
                    "filename_template": lambda v: f"nychdb_cdta_{v}_csv.zip",
                },
                "data_dictionary": {
                    "filename": "Housing_Summary_Files_Data_Dictionary.xlsx",
                },
            },
        },
        "housing_database_by_2024_city_council": {
            "bytes_dataset_key": "housing-unit-change-summary",
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nychdb_council_{v}_shp.zip",
                },
                "csv": {
                    "filename_template": lambda v: f"nychdb_council_{v}_csv.zip",
                },
                "data_dictionary": {
                    "filename": "Housing_Summary_Files_Data_Dictionary.xlsx",
                },
            },
        },
        "housing_database_by_community_district": {
            "bytes_dataset_key": "housing-unit-change-summary",
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nychdb_community_{v}_shp.zip",
                },
                "csv": {
                    "filename_template": lambda v: f"nychdb_council_{v}_csv.zip",
                },
                "data_dictionary": {
                    "filename": "Housing_Summary_Files_Data_Dictionary.xlsx",
                },
            },
        },
    },
    "e_designations": {
        "e_designations": {
            "files": {
                "csv_package": {
                    "filename_template": lambda v: f"nyedes_{v}csv.zip",
                },
                "shapefile": {
                    "filename_template": lambda v: f"nyedes_{v}shp.zip",
                },
                "nyedes_metadata.pdf": {
                    "filename": "nyedes_metadata.pdf",
                },
            },
            "bytes_dataset_key": "e-designations",
        }
    },
    "facilities": {
        "facilities": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"facilities_{v}_shp.zip",
                },
                "csv": {
                    "filename_template": lambda v: f"facilities_{v}_csv.zip",
                },
                "data_dictionary": {
                    "filename": "facilities_datadictionary.xlsx",
                },
                "readme": {
                    "filename": "facilities_readme.pdf",
                },
            },
            "bytes_dataset_key": "facilities",
            "file_resource_override": "facilities-database",
        }
    },
    "fresh_zoning_boundary": {
        "fresh_zoning_boundary": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nycfreshzoning_{v}.zip",
                },
                "metadata_pdf": {
                    "filename": "nycfreshzoning_metadata.pdf",
                },
            },
            "bytes_dataset_key": "fresh",
        }
    },
    "inclusionary_housing_designated_areas": {
        "inclusionary_housing_designated_areas": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nycihda_{v}.zip",
                },
                "metadata_pdf": {
                    "filename": "nycihda_metadata.pdf",
                },
            }
        }
    },
    "lion": {
        "2010_census_blocks": {
            "files": {
                "shapefile_water_not_included": {
                    "filename_template": lambda v: f"nycb2010_{v}.zip",
                },
                "shapefile_water_included": {
                    "filename_template": lambda v: f"nycb2010wi_{v}.zip",
                },
                "nycb2010_metadata.pdf": {
                    "filename": "nycb2010_metadata.pdf",
                },
                "nycb2010wi_metadata.pdf": {
                    "filename": "nycb2010wi_metadata.pdf",
                },
            },
            "bytes_dataset_key": "census-blocks",
        },
        "2010_census_tracts": {
            "files": {
                "shapefile_water_not_included": {
                    "filename_template": lambda v: f"nyct2010_{v}.zip",
                },
                "shapefile_water_included": {
                    "filename_template": lambda v: f"nyct2010wi_{v}.zip",
                },
                "nyct2010_metadata.pdf": {
                    "filename": "nyct2010_metadata.pdf",
                },
                "nyct2010wi_metadata.pdf": {
                    "filename": "nyct2010wi_metadata.pdf",
                },
            },
            "bytes_dataset_key": "census-tracts",
        },
        "2010_neighborhood_tabulation_areas": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nynta2010_{v}.zip",
                },
                "nynta2010_metadata.pdf": {
                    "filename": "nynta2010_metadata.pdf",
                },
            },
            "bytes_dataset_key": "neighborhood-tabulation",
            "file_resource_override": "neighborhood-tabulation-areas",
        },
        "2010_public_use_microdata_areas": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nypuma2010_{v}.zip",
                },
                "nypuma2010_metadata.pdf": {
                    "filename": "nypuma2010_metadata.pdf",
                },
            },
            "bytes_dataset_key": "public-use-microdata-areas",
            "file_resource_override": "puma",
        },
        "2020_census_blocks": {
            "files": {
                "shapefile_water_not_included": {
                    "filename_template": lambda v: f"nycb2020_{v}.zip",
                },
                "shapefile_water_included": {
                    "filename_template": lambda v: f"nycb2020wi_{v}.zip",
                },
                "nycb2020_metadata.pdf": {
                    "filename": "nycb2020_metadata.pdf",
                },
                "nycb2020wi_metadata.pdf": {
                    "filename": "nycb2020wi_metadata.pdf",
                },
            },
            "bytes_dataset_key": "census-blocks",
        },
        "2020_census_tracts": {
            "files": {
                "shapefile_water_not_included": {
                    "filename_template": lambda v: f"nyct2020_{v}.zip",
                },
                "shapefile_water_included": {
                    "filename_template": lambda v: f"nyct2020wi_{v}.zip",
                },
                "nyct2020_metadata.pdf": {
                    "filename": "nyct2020_metadata.pdf",
                },
                "nyct2020wi_metadata.pdf": {
                    "filename": "nyct2020wi_metadata.pdf",
                },
            },
            "bytes_dataset_key": "census-tracts",
        },
        "2020_community_district_tabulation_areas": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nycdta2020_{v}.zip",
                },
                "nycdta2020_metadata.pdf": {
                    "filename": "nycdta2020_metadata.pdf",
                },
            },
            "bytes_dataset_key": "community-district-tabulation",
            "file_resource_override": "community-district-tabulation-areas",
        },
        "2020_neighborhood_tabulation_areas": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nynta2020_{v}.zip",
                },
                "nynta2020_metadata.pdf": {
                    "filename": "nynta2020_metadata.pdf",
                },
            },
            "bytes_dataset_key": "neighborhood-tabulation",
            "file_resource_override": "neighborhood-tabulation-areas",
        },
        "2020_public_use_microdata_areas": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nypuma2020_{v}.zip",
                },
                "metadata_pdf": {
                    "filename": "nypuma2020_metadata.pdf",
                },
            },
            "bytes_dataset_key": "public-use-microdata-areas",
            "file_resource_override": "puma",
        },
        "atomic_polygons": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nyap_{v}.zip",
                },
                "nyap_metadata.pdf": {
                    "filename": "nyap_metadata.pdf",
                },
            },
            "bytes_dataset_key": "atomic-polygons",
        },
        "borough_boundaries": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nybb_{v}.zip",
                },
                "shapefile_wi": {
                    "filename_template": lambda v: f"nybbwi_{v}.zip",
                },
                "nybb_metadata.pdf": {
                    "filename": "nybb_metadata.pdf",
                },
                "nybbwi_metadata.pdf": {
                    "filename": "nybbwi_metadata.pdf",
                },
            },
            "bytes_dataset_key": "borough-boundaries",
        },
        "city_council_districts": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nycc_{v}.zip",
                },
                "shapefile_wi": {
                    "filename_template": lambda v: f"nyccwi_{v}.zip",
                },
                "nycc_metadata.pdf": {
                    "filename": "nycc_metadata.pdf",
                },
                "nyccwi_metadata.pdf": {
                    "filename": "nyccwi_metadata.pdf",
                },
            },
            "bytes_dataset_key": "city-council",
        },
        "community_districts": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nycd_{v}.zip",
                },
                "shapefile_wi": {
                    "filename_template": lambda v: f"nycdwi_{v}.zip",
                },
                "nycd_metadata.pdf": {
                    "filename": "nycd_metadata.pdf",
                },
                "nycdwi_metadata.pdf": {
                    "filename": "nycdwi_metadata.pdf",
                },
            },
            "bytes_dataset_key": "community-districts",
        },
        "congressional_districts": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nycg_{v}.zip",
                },
                "shapefile_wi": {
                    "filename_template": lambda v: f"nycgwi_{v}.zip",
                },
                "nycg_metadata.pdf": {
                    "filename": "nycg_metadata.pdf",
                },
                "nycgwi_metadata.pdf": {
                    "filename": "nycgwi_metadata.pdf",
                },
            },
            "bytes_dataset_key": "congressional-districts",
            "file_resource_override": "congressional",
        },
        "election_districts": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nyed_{v}.zip",
                },
                "shapefile_wi": {
                    "filename_template": lambda v: f"nyedwi_{v}.zip",
                },
                "nyed_metadata.pdf": {
                    "filename": "nyed_metadata.pdf",
                },
                "nyedwi_metadata.pdf": {
                    "filename": "nyedwi_metadata.pdf",
                },
            },
            "bytes_dataset_key": "election-districts",
        },
        "fire_battalions": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nyfb_{v}.zip",
                },
                "nyfb_metadata.pdf": {
                    "filename": "nyfb_metadata.pdf",
                },
            },
            "bytes_dataset_key": "fire-battalions-companies-divisions",
            "file_resource_override": "fire-battalions",
        },
        "fire_companies": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nyfc_{v}.zip",
                },
                "nyfc_metadata.pdf": {
                    "filename": "nyfc_metadata.pdf",
                },
            },
            "bytes_dataset_key": "fire-battalions-companies-divisions",
            "file_resource_override": "fire-companies",
        },
        "fire_divisions": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nyfd_{v}.zip",
                },
                "nyfd_metadata.pdf": {
                    "filename": "nyfd_metadata.pdf",
                },
            },
            "bytes_dataset_key": "fire-battalions-companies-divisions",
            "file_resource_override": "fire-divisions",
        },
        "health_areas": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nyha_{v}.zip",
                },
                "nyha_metadata.pdf": {
                    "filename": "nyha_metadata.pdf",
                },
            },
            "file_resource_override": "health-area",
            "bytes_dataset_key": "health-area-center",
        },
        "health_center_districts": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nyhc_{v}.zip",
                },
                "nyhc_metadata.pdf": {
                    "filename": "nyhc_metadata.pdf",
                },
            },
            "file_resource_override": "health-center",
            "bytes_dataset_key": "health-area-center",
        },
        "hurricane_evacuation_zones": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nyhez_{v}.zip",
                },
                "nyhez_metadata.pdf": {
                    "filename": "nyhez_metadata.pdf",
                },
            },
            "bytes_dataset_key": "hurricane-evacuation-zones",
        },
        "lion": {
            "files": {
                "lion_zip": {
                    "filename_template": lambda v: f"nyclion_{v}.zip",
                },
                "lion_metadata.pdf": {
                    "filename": "lion_metadata.pdf",
                },
                "altnames_metadata.pdf": {
                    "filename": "altnames_metadata.pdf",
                },
                "node_stname_metadata.pdf": {
                    "filename": "node_stname_metadata.pdf",
                },
                "node_metadata.pdf": {
                    "filename": "node_metadata.pdf",
                },
            },
            "bytes_dataset_key": "lion",
        },
        "lion_differences_file": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"ldf_{v}.zip",
                },
                "ldf_metadata.pdf": {
                    "filename": "ldf_metadata.pdf",
                },
            },
            "bytes_dataset_key": "lion-difference-files",
            "catalog_file_override": "lion-difference-files",
            "file_resource_override": "lion-differences-file",
        },
        "municipal_court_districts": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nymc_{v}.zip",
                },
                "shapefile_wi": {
                    "filename_template": lambda v: f"nymcwi_{v}.zip",
                },
                "nymc_metadata.pdf": {
                    "filename": "nymc_metadata.pdf",
                },
                "nymcwi_metadata.pdf": {
                    "filename": "nymcwi_metadata.pdf",
                },
            },
            "bytes_dataset_key": "municipal-court",
            "catalog_file_override": "municipal-courts",
            "file_resource_override": "municipal-court",
        },
        "police_precincts": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nypp_{v}.zip",
                },
                "nypp_metadata.pdf": {
                    "filename": "nypp_metadata.pdf",
                },
            },
            "bytes_dataset_key": "police-precincts",
        },
        "property_address_directory": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"pad_{v}.zip",
                },
                "padlayout.pdf": {
                    "filename": "padlayout.pdf",
                },
                "padgui.pdf": {
                    "filename": "padgui.pdf",
                },
            },
            "bytes_dataset_key": "pad",
        },
        "pseudo_lots": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"pseudolots_{v}.zip",
                },
                "pseudolots_datadictionary.xlsx": {
                    "filename": "pseudolots_datadictionary.xlsx",
                },
            },
            "bytes_dataset_key": "pseudo-lots",
            "file_resource_override": "pseudolots",
        },
        "roadbed_pointer_list": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"rpl_{v}.zip",
                },
                "rpl_filelayout.pdf": {
                    "filename": "rpl_filelayout.pdf",
                },
            },
            "bytes_dataset_key": "road-pointer-list",
            "file_resource_override": "rpl",
        },
        "school_districts": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nysd_{v}.zip",
                },
                "nysd_metadata.pdf": {
                    "filename": "nysd_metadata.pdf",
                },
            },
            "bytes_dataset_key": "school-districts",
            "file_resource_override": "school",
        },
        "state_assembly_districts": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nyad_{v}.zip",
                },
                "shapefile_wi": {
                    "filename_template": lambda v: f"nyadwi_{v}.zip",
                },
                "nyad_metadata.pdf": {
                    "filename": "nyad_metadata.pdf",
                },
                "nyadwi_metadata.pdf": {
                    "filename": "nyadwi_metadata.pdf",
                },
            },
            "bytes_dataset_key": "state-assembly",
        },
        "state_senate_districts": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nyss_{v}.zip",
                },
                "shapefile_wi": {
                    "filename_template": lambda v: f"nysswi_{v}.zip",
                },
                "nyss_metadata.pdf": {
                    "filename": "nyss_metadata.pdf",
                },
                "nysswi_metadata.pdf": {
                    "filename": "nysswi_metadata.pdf",
                },
            },
            "bytes_dataset_key": "state-senate",
        },
        "street_name_dictionary": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"snd_{v}.zip",
                },
                "snd_metadata.pdf": {
                    "filename": "snd_metadata.pdf",
                },
                "snd_userguide.pdf": {
                    "filename": "snd_userguide.pdf",
                },
            },
            "bytes_dataset_key": "snd",
        },
    },
    "lower_density_growth_management_areas": {
        "lower_density_growth_management_areas": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nycldgma_{v}.zip",
                },
                "metadata_pdf": {
                    "filename": "nycldgma_metadata.pdf",
                },
            },
            "bytes_dataset_key": "lower-density-growth-management-area",
        }
    },
    "mandatory_inclusionary_housing": {
        "mandatory_inclusionary_housing": {
            "files": {
                "primary_shapefile": {
                    "filename_template": lambda v: f"nycmih_{v}.zip",
                },
                "nycmih_metadata.pdf": {
                    "filename": "nycmih_metadata.pdf",
                },
            },
            "bytes_dataset_key": "mandatory-inclusionary-housing",
        }
    },
    "new_york_city_neighborhood_name_centroid": {
        "new_york_city_neighborhood_name_centroid": {
            "files": {
                "shapefile": {
                    "url_middle": "http://www1.nyc.gov/assets/planning/download/zip/data-maps/",
                    "filename_template": lambda v: f"nyc_nhoodsnames{v}.zip",
                },
                "readme_pdf": {
                    "url_base_override": PLANNING_ASSETS_URL_BASE,
                    "url_middle": "pdf/data-maps/",
                    "filename": "meta_nhood.pdf",
                },
            },
            "bytes_dataset_key": "neighborhood-names",
        }
    },
    "pluto": {
        "change_file": {
            "files": {
                "csv_package": {
                    "filename_template": lambda v: f"PLUTOChangeFile{v}.zip",
                },
                "pluto_change_file_readme": {
                    "filename": "plutochangefile_readme.pdf",
                },
            },
            "bytes_dataset_key": "mappluto-pluto-change",
            "file_resource_override": "pluto-change-file",
        },
        "pluto": {
            "files": {
                "csv_zip": {
                    "filename_template": lambda v: f"nyc_pluto_{v}_csv.zip",
                },
                "pluto_datadictionary.pdf": {
                    "filename": "pluto_datadictionary.pdf",
                },
                "pluto_readme.pdf": {
                    "filename": "pluto_readme.pdf",
                },
            },
            "bytes_dataset_key": "mappluto-pluto-change",
            "file_resource_override": "pluto",
        },
    },
    "pops": {
        "pops": {
            "files": {
                "primary_csv_zip": {
                    "filename_template": lambda v: f"pops_{v}_csv.zip",
                },
                "primary_shapefile": {
                    "filename_template": lambda v: f"pops_{v}_shp.zip",
                },
                "pops_datadictionary_xlsx": {
                    "filename": "pops_datadictionary.xlsx",
                },
            },
            "bytes_dataset_key": "privately-owned-public-space",
            "catalog_file_override": "privately-owned-public-spaces",
        }
    },
    "projected_sea_level_rise": {
        "projected_sea_level_rise": {
            "no_archived_versions": True,
            "files": {
                "fgdb": {
                    "url_middle": "https://www.nyc.gov/assets/planning/download/zip/data-maps/",
                    "filename_template": lambda v: f"nyc-future-high-tides-with-slr-{v}.zip",
                },
                "metadata_pdf": {
                    "url_middle": "https://www.nyc.gov/assets/planning/download/pdf/data-maps/",
                    "filename": "slr_metadata.pdf",
                },
            },
            "bytes_dataset_key": "future-hide-tides",
        }
    },
    "transit_zones": {
        "transit_zones": {
            "files": {
                "shapefile": {
                    "filename_template": lambda v: f"nyctransitzones_{v}.zip",
                },
                "readme_pdf": {
                    "filename": "nyctransitzones_metadata.pdf",
                },
            }
        }
    },
    "waterfront_public_access_areas": {
        "hpb_launches": {
            "files": {
                "readme_pdf": {
                    "filename": "WAM_hpb_launches_metadata.pdf",
                }
            },
            "bytes_dataset_key": "nyc-waterfront-access-map",
        },
        "nyc_saltwaterfishingsites": {
            "files": {
                "readme_pdf": {
                    "filename": "WAM_nyc_saltwaterfishingsites_metadata.pdf",
                }
            },
            "bytes_dataset_key": "nyc-waterfront-access-map",
        },
        "publicly_owned_waterfront": {
            "files": {
                "readme_pdf": {
                    "filename": "WAM_publiclyownedwaterfront_metadata.pdf",
                }
            },
            "bytes_dataset_key": "nyc-waterfront-access-map",
        },
        "waterfront_public_access_areas": {
            "files": {
                "fgdb": {
                    "filename_template": lambda v: f"nycwpaas_{v}fgdb.zip",
                }
            },
            "bytes_dataset_key": "nyc-waterfront-access-map",
        },
        "wpaas": {
            "files": {
                "readme_pdf": {
                    "filename": "WAM_wpaas_metadata.pdf",
                }
            },
            "bytes_dataset_key": "nyc-waterfront-access-map",
        },
        "wpaas_accesspoints": {
            "files": {
                "readme_pdf": {
                    "filename": "WAM_wpaas_accesspoints_metadata.pdf",
                }
            },
            "bytes_dataset_key": "nyc-waterfront-access-map",
        },
        "wpaas_footprints": {
            "files": {
                "readme_pdf": {
                    "filename": "WAM_wpaas_footprints_metadata.pdf",
                }
            },
            "bytes_dataset_key": "nyc-waterfront-access-map",
        },
    },
    "waterfront_revitalization_program": {
        "waterfront_revitalization_program": {
            "files": {
                "shapefile": {
                    "url_middle": "http://www1.nyc.gov/assets/planning/download/zip/data-maps/",
                    "filename_template": lambda v: f"nycwrpgdb{v}.zip",
                },
                "readme_pdf": {
                    "url_base_override": PLANNING_ASSETS_URL_BASE,
                    "url_middle": "pdf/data-maps/",
                    "filename": "meta_wap.pdf",
                },
            },
            "no_archived_versions": True,
            "bytes_dataset_key": "wrp-coastal-zone-boundary",
        }
    },
    "zap": {
        "projects": {
            "files": {
                "csv_package": {
                    "filename_template": lambda v: f"zapprojects_{v}csv.zip",
                }
            },
            "bytes_dataset_key": "zoning-application-portal",
            "file_resource_override": "zap",
        },
        "bbls": {
            "files": {
                "csv_package": {
                    "filename_template": lambda v: f"zapprojectbbls_{v}csv.zip",
                }
            },
            "bytes_dataset_key": "zoning-application-portal",
            "file_resource_override": "zap",
        },
    },
    "zoning": {
        "commercial_overlay_district": {
            "files": {
                "dcm_multilayer": {
                    "filename_template": lambda v: f"nycgiszoningfeatures_{v}shp.zip",
                },
                "metadata_pdf": {
                    "filename": "nyco_metadata.pdf",
                },
            },
            "bytes_dataset_key": "gis-zoning-features",
        },
        "zoning_districts": {
            "files": {
                "dcm_multilayer": {
                    "filename_template": lambda v: f"nycgiszoningfeatures_{v}shp.zip",
                },
                "metadata_pdf": {
                    "filename": "nyzd_metadata.pdf",
                },
            },
            "bytes_dataset_key": "gis-zoning-features",
        },
        "zoning_map_amendments": {
            "files": {
                "dcm_multilayer": {
                    "filename_template": lambda v: f"nycgiszoningfeatures_{v}shp.zip",
                },
                "metadata_pdf": {
                    "filename": "nyzma_metadata.pdf",
                },
            },
            "bytes_dataset_key": "gis-zoning-features",
        },
        "special_purpose_districts_subdistricts": {
            "files": {
                "dcm_multilayer": {
                    "filename_template": lambda v: f"nycgiszoningfeatures_{v}shp.zip",
                },
                "metadata_pdf": {
                    "filename": "nysp_sd_metadata.pdf",
                },
            },
            "bytes_dataset_key": "gis-zoning-features",
        },
        "zoning_features": {
            "files": {
                "fgdb": {
                    "filename_template": lambda v: f"nycgiszoningfeatures_{v}fgdb.zip",
                },
                "nyco_metadata": {
                    "filename": "nyco_metadata.pdf",
                },
                "nylh_metadata": {
                    "filename": "nylh_metadata.pdf",
                },
                "nysp_metadata": {
                    "filename": "nysp_metadata.pdf",
                },
                "nysp_sd_metadata": {
                    "filename": "nysp_sd_metadata.pdf",
                },
                "nyzd_metadata": {
                    "filename": "nyzd_metadata.pdf",
                },
                "nyzma_metadata": {
                    "filename": "nyzma_metadata.pdf",
                },
            },
            "bytes_dataset_key": "gis-zoning-features",
        },
        "limited_height_districts": {
            "files": {
                "dcm_multilayer": {
                    "filename_template": lambda v: f"nycgiszoningfeatures_{v}shp.zip",
                },
                "metadata_pdf": {
                    "filename": "nylh_metadata.pdf",
                },
            },
            "bytes_dataset_key": "gis-zoning-features",
        },
        "special_purpose_districts": {
            "files": {
                "dcm_multilayer": {
                    "filename_template": lambda v: f"nycgiszoningfeatures_{v}shp.zip",
                },
                "metadata_pdf": {
                    "filename": "nysp_metadata.pdf",
                },
            },
            "bytes_dataset_key": "gis-zoning-features",
        },
        "georeferenced_nyc_zoning_maps": {
            "files": {
                "fgdb": {
                    "filename_template": lambda v: f"georeferencedzoningmaps_{v}.zip",
                },
                "georeferencedzoningmaps_metadata": {
                    "filename": "georeferencedzoningmaps_metadata.pdf",
                },
            },
            "bytes_dataset_key": "georeferenced-map",
            "catalog_file_override": "georeferenced-maps",
        },
    },
    "ztl": {
        "ztl": {
            "files": {
                "csv_package": {
                    "filename_template": lambda v: f"nyczoningtaxlotdb_{v}.zip",
                }
            },
            "bytes_dataset_key": "zoning-taxlot-database",
            "catalog_file_override": "zoning-tax-lot-database",
        }
    },
}
