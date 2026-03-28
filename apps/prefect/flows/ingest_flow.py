import os
import time
from typing import Dict, Any

from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact
from minio import Minio
from minio.error import S3Error


@task
def validate_dataset_task(dataset_name: str) -> Dict[str, Any]:
    logger = get_run_logger()
    logger.info(f"Validating dataset: {dataset_name}")

    time.sleep(1)

    validation_result = {
        "dataset_name": dataset_name,
        "record_count": 12500,
        "validation_timestamp": time.time(),
    }

    logger.info(f"Validation completed for {dataset_name}")
    return validation_result


@task
def transform_dataset_task(dataset_name: str, validation_result: Dict[str, Any]) -> str:
    logger = get_run_logger()
    logger.info(f"Transforming dataset: {dataset_name}")

    time.sleep(1)

    output_path = f"/tmp/ingest/{dataset_name}/{int(time.time())}"
    os.makedirs(output_path, exist_ok=True)

    processed_file = f"{output_path}/{dataset_name}_processed.parquet"
    with open(processed_file, "w") as f:
        f.write(f"Mock parquet data for {dataset_name}\n")

    logger.info(f"Transformation completed: {processed_file}")
    return output_path


@task
def store_dataset_task(dataset_name: str, output_path: str) -> str:
    logger = get_run_logger()
    logger.info(f"Storing dataset: {dataset_name}")

    time.sleep(1)

    minio_client = Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False,
    )

    bucket_name = "ingest"

    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            logger.info(f"Created bucket {bucket_name}")
    except S3Error as e:
        logger.error(f"Error creating bucket: {e}")

    object_key = f"{dataset_name}/latest/{dataset_name}_processed.parquet"
    file_path = f"{output_path}/{dataset_name}_processed.parquet"

    try:
        minio_client.fput_object(bucket_name, object_key, file_path)
        logger.info(f"Uploaded {file_path} to {bucket_name}/{object_key}")
    except S3Error as e:
        logger.error(f"Error uploading file: {e}")
        raise

    storage_path = f"s3://{bucket_name}/{object_key}"
    logger.info(f"Dataset stored: {storage_path}")
    return storage_path


@flow(name="Ingest Dataset")
def ingest_flow(dataset_name: str):
    logger = get_run_logger()
    logger.info(f"Starting ingest process for dataset: {dataset_name}")

    validation_result = validate_dataset_task(dataset_name)
    output_path = transform_dataset_task(dataset_name, validation_result)
    storage_path = store_dataset_task(dataset_name, output_path)

    create_markdown_artifact(
        key=f"ingest-summary-{dataset_name}",
        markdown=f"""# Ingest Summary: {dataset_name}""",
    )

    logger.info(f"Ingest completed for {dataset_name}: {storage_path}")
    return storage_path
