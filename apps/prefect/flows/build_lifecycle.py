import os
import time
from typing import Dict, Any

from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact
from minio import Minio
from minio.error import S3Error


@task
def plan_task(recipe_config: Dict[str, Any]) -> Dict[str, Any]:
    logger = get_run_logger()
    logger.info(f"Planning build for {recipe_config['product']}")

    time.sleep(2)

    planned_recipe = {
        **recipe_config,
        "resolved_version": recipe_config["version"],
        "inputs_resolved": {
            "bpl_libraries": {
                "dataset": "dcp_libraries",
                "version": "20241101",
                "file_type": "csv",
                "destination": "file",
            }
        },
        "plan_timestamp": time.time(),
    }

    logger.info("Planning completed successfully")
    return planned_recipe


@task
def load_task(planned_recipe: Dict[str, Any]) -> str:
    logger = get_run_logger()
    logger.info("Loading source data (mocked)")

    time.sleep(3)

    data_path = (
        f"/tmp/data/{planned_recipe['product']}/{planned_recipe['resolved_version']}"
    )
    os.makedirs(data_path, exist_ok=True)

    mock_data_file = f"{data_path}/libraries.csv"
    with open(mock_data_file, "w") as f:
        f.write("id,name,address,borough\n")
        f.write("1,Central Library,Grand Army Plaza,Brooklyn\n")
        f.write("2,Jefferson Market Library,425 Avenue of the Americas,Manhattan\n")

    logger.info(f"Mock data loaded to {data_path}")
    return data_path


@task
def build_task(data_path: str, planned_recipe: Dict[str, Any]) -> str:
    logger = get_run_logger()
    logger.info("Running build process (mocked DBT)")

    time.sleep(4)

    build_output_path = f"{data_path}/build_outputs"
    os.makedirs(build_output_path, exist_ok=True)

    output_file = f"{build_output_path}/bpl_libraries_processed.csv"
    with open(output_file, "w") as f:
        f.write("library_id,library_name,full_address,borough_code\n")
        f.write("BPL001,Central Library,Grand Army Plaza Brooklyn NY,BK\n")
        f.write(
            "NYPL002,Jefferson Market Library,425 Avenue of the Americas Manhattan NY,MN\n"
        )

    logger.info(f"Build completed, outputs at {build_output_path}")
    return build_output_path


@task
def package_task(build_output_path: str, planned_recipe: Dict[str, Any]) -> str:
    logger = get_run_logger()
    logger.info("Packaging build outputs")

    time.sleep(2)

    minio_client = Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False,
    )

    bucket_name = os.getenv("MINIO_BUCKET", "builds")

    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            logger.info(f"Created bucket {bucket_name}")
    except S3Error as e:
        logger.error(f"Error creating bucket: {e}")

    object_key = f"{planned_recipe['product']}/{planned_recipe['resolved_version']}/bpl_libraries_processed.csv"
    file_path = f"{build_output_path}/bpl_libraries_processed.csv"

    try:
        minio_client.fput_object(bucket_name, object_key, file_path)
        logger.info(f"Uploaded {file_path} to {bucket_name}/{object_key}")
    except S3Error as e:
        logger.error(f"Error uploading file: {e}")
        raise

    package_path = f"s3://{bucket_name}/{object_key}"
    logger.info(f"Packaging completed: {package_path}")
    return package_path


@flow(name="Build Lifecycle")
def build_lifecycle_flow(recipe_path: str = "/app/config/recipe.yml"):
    logger = get_run_logger()
    logger.info("Starting build lifecycle")

    recipe_config = {
        "product": "bpl_libraries",
        "version": "2024.12.8",
        "inputs": {
            "bpl_libraries": {
                "dataset": "dcp_libraries",
                "version": "20241101",
                "destination": "file",
            }
        },
    }

    planned_recipe = plan_task(recipe_config)
    data_path = load_task(planned_recipe)
    build_output_path = build_task(data_path, planned_recipe)
    package_path = package_task(build_output_path, planned_recipe)

    create_markdown_artifact(
        key="build-summary",
        markdown=f"""# Build Summary
        
## Product: {planned_recipe["product"]}
## Version: {planned_recipe["resolved_version"]}

### Stages Completed:
- ✅ **Plan**: Recipe resolved successfully
- ✅ **Load**: Mock data loaded to `{data_path}`
- ✅ **Build**: DBT build simulated, outputs created
- ✅ **Package**: Artifacts uploaded to `{package_path}`

### Timeline:
- Total execution time: ~11 seconds (mocked)
- Plan: 2s | Load: 3s | Build: 4s | Package: 2s
        """,
    )

    logger.info(f"Build lifecycle completed successfully: {package_path}")
    return package_path


if __name__ == "__main__":
    build_lifecycle_flow()
