import os
import time
from typing import Dict, Any, List

from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact


@task
def prepare_distribution_task(dataset_name: str) -> Dict[str, Any]:
    logger = get_run_logger()
    logger.info(f"Preparing distribution for dataset: {dataset_name}")

    time.sleep(1)

    distribution_config = {
        "dataset_name": dataset_name,
        "targets": ["socrata", "arcgis", "ftp"],
        "formats": ["csv", "geojson", "shapefile"],
        "metadata_updated": True,
        "prep_timestamp": time.time(),
    }

    logger.info(f"Distribution preparation completed for {dataset_name}")
    return distribution_config


@task
def publish_to_socrata_task(dataset_name: str) -> str:
    logger = get_run_logger()
    logger.info(f"Publishing {dataset_name} to Socrata")

    time.sleep(2)

    socrata_id = f"abc{hash(dataset_name) % 1000:03d}-xyz9"
    logger.info(f"Published to Socrata: {socrata_id}")
    return f"https://data.cityofnewyork.us/d/{socrata_id}"


@task
def publish_to_arcgis_task(dataset_name: str) -> str:
    logger = get_run_logger()
    logger.info(f"Publishing {dataset_name} to ArcGIS Online")

    time.sleep(2)

    service_id = f"{dataset_name.lower()}_service_{hash(dataset_name) % 10000}"
    logger.info(f"Published to ArcGIS: {service_id}")
    return f"https://services.arcgis.com/fHM9e57cFHacJ8Wb/arcgis/rest/services/{service_id}/FeatureServer"


@task
def publish_to_ftp_task(dataset_name: str) -> str:
    logger = get_run_logger()
    logger.info(f"Publishing {dataset_name} to FTP")

    time.sleep(1)

    ftp_path = f"/public/datasets/{dataset_name.lower()}/{dataset_name}_latest.zip"
    logger.info(f"Published to FTP: {ftp_path}")
    return f"ftp://ftp.nyc.gov{ftp_path}"


@task
def update_metadata_task(
    dataset_name: str, publication_urls: List[str]
) -> Dict[str, Any]:
    logger = get_run_logger()
    logger.info(f"Updating metadata for {dataset_name}")

    time.sleep(1)

    metadata = {
        "dataset_name": dataset_name,
        "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "distribution_urls": publication_urls,
        "status": "published",
        "download_count": hash(dataset_name) % 50000,
        "metadata_timestamp": time.time(),
    }

    logger.info(f"Metadata updated for {dataset_name}")
    return metadata


@flow(name="Distribute Dataset")
def distribute_flow(dataset_name: str):
    logger = get_run_logger()
    logger.info(f"Starting distribution process for dataset: {dataset_name}")

    distribution_config = prepare_distribution_task(dataset_name)

    socrata_url = publish_to_socrata_task(dataset_name)
    arcgis_url = publish_to_arcgis_task(dataset_name)
    ftp_url = publish_to_ftp_task(dataset_name)

    publication_urls = [socrata_url, arcgis_url, ftp_url]
    metadata = update_metadata_task(dataset_name, publication_urls)

    create_markdown_artifact(
        key=f"distribution-summary-{dataset_name}",
        markdown=f"""# Distribution Summary: {dataset_name}... """,
    )

    logger.info(f"Distribution completed for {dataset_name}")
    return {
        "dataset_name": dataset_name,
        "publication_urls": publication_urls,
        "metadata": metadata,
    }
