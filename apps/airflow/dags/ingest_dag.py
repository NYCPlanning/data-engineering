from pathlib import Path
from typing import List

from airflow.sdk import dag, task
from airflow.sdk.definitions.asset import Asset


PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
INGEST_TEMPLATES_PATH = PROJECT_ROOT / "ingest_templates"


def get_ingest_template_ids() -> List[str]:
    templates = []
    for file in INGEST_TEMPLATES_PATH.glob("*.yml"):
        templates.append(file.stem)
    return sorted(templates)


def create_ingest_dag(template_id: str):
    dag_id = f"ingest_{template_id}"

    @dag(
        dag_id=dag_id,
        schedule=None,
        catchup=False,
        tags=["ingest", template_id],
    )
    def ingest_dag():
        ingest_asset = Asset(
            name=f"edm-recipes/datasets/{template_id}",
            uri=f"s3://edm-recipes/datasets/{template_id}/latest/{template_id}.parquet",
            group="edm-recipes-datasets",
        )

        @task(outlets=[ingest_asset])
        def ingest_task():
            from dcpy.lifecycle.ingest.run import ingest

            version = "default"
            output_path = (
                INGEST_TEMPLATES_PATH.parent.parent
                / ".lifecycle"
                / "ingest"
                / template_id
                / version
            )
            output_path.mkdir(parents=True, exist_ok=True)

            ingest(
                dataset_id=template_id,
                version=version,
                push=False,
                definition_dir=INGEST_TEMPLATES_PATH,
            )

            return {"output_path": str(output_path), "version": version}

        return ingest_task()

    return ingest_dag()


template_ids = get_ingest_template_ids()
for template_id in template_ids:
    create_ingest_dag(template_id)
