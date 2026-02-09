from pathlib import Path
from typing import Any, Dict, List
import subprocess
import json

from airflow.sdk import dag, task, task_group, Asset
import yaml


PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
PRODUCTS_PATH = PROJECT_ROOT / "products" / "products.yml"
LIFECYCLE_ROOT = PROJECT_ROOT / ".lifecycle" / "builds"


def load_products() -> List[Dict[str, Any]]:
    with open(PRODUCTS_PATH) as f:
        data = yaml.safe_load(f)
    return data["products"]


def create_build_dag(product: Dict[str, Any]):
    product_id = product["id"]
    build_command = product["build_command"]
    dag_id = f"build_{product_id.replace(' ', '_').replace('-', '_')}"

    with open(PROJECT_ROOT / "products" / product_id / "recipe.yml") as f:
        data = yaml.safe_load(f)
    input_datasets = [ds["name"] for ds in data["inputs"]["datasets"]]
    input_datasets = [
        Asset(
            name=f"edm-recipes/datasets/{ds}",
            uri=f"s3://edm-recipes/datasets/{ds}/latest/{ds}.parquet",
            group="edm-recipes-datasets",
        )
        for ds in input_datasets
    ]

    @dag(
        dag_id=dag_id,
        schedule=input_datasets,
        catchup=False,
        tags=["build", product_id],
    )
    def build_dag():
        partition_key = "default"
        output_dir = LIFECYCLE_ROOT / product_id / partition_key
        output_dir.mkdir(parents=True, exist_ok=True)

        @task
        def plan():
            plan_file = output_dir / "plan.json"
            plan_data = {
                "product_id": product_id,
                "partition_key": partition_key,
                "stage": "plan",
                "output_dir": str(output_dir),
            }
            plan_file.write_text(json.dumps(plan_data, indent=2))
            return str(plan_file)

        @task
        def load(plan_file):
            load_file = output_dir / "load.json"
            load_data = {
                "product_id": product_id,
                "partition_key": partition_key,
                "stage": "load",
                "output_dir": str(output_dir),
            }
            load_file.write_text(json.dumps(load_data, indent=2))
            return str(load_file)

        @task
        def build(load_file):
            build_file = output_dir / "build.json"
            result = subprocess.run(
                build_command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=str(output_dir),
            )
            build_data = {
                "product_id": product_id,
                "partition_key": partition_key,
                "stage": "build",
                "output_dir": str(output_dir),
                "return_code": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
            build_file.write_text(json.dumps(build_data, indent=2))
            return str(build_file)

        @task
        def package(build_file):
            package_file = output_dir / "package.json"
            package_data = {
                "product_id": product_id,
                "partition_key": partition_key,
                "stage": "package",
                "output_dir": str(output_dir),
            }
            package_file.write_text(json.dumps(package_data, indent=2))
            return str(package_file)

        plan_result = plan()
        load_result = load(plan_result)
        build_result = build(load_result)
        package_result = package(build_result)

        return package_result

    return build_dag()


products = load_products()
build_dags = [create_build_dag(product) for product in products]
