import json
from dcpy.connectors import s3

RECIPES_BUCKET = "edm-recipes"


def get_config(name, version="latest"):
    """Retrieve a recipe config from s3."""
    obj = s3.client().get_object(
        Bucket=RECIPES_BUCKET, Key=f"datasets/{name}/{version}/config.json"
    )
    file_content = str(obj["Body"].read(), "utf-8")
    return json.loads(file_content)


def fetch_sql(name: str, version: str, local_library_dir):
    """Retrieve SQL dump file from edm-recipes. Returns fetched file's path."""
    target_dir = local_library_dir / "datasets" / name / version
    target_file_path = target_dir / (name + ".sql")
    if (target_file_path).exists():
        print(f"✅ {name}.sql exists in cache")
    else:
        if not target_dir.exists():
            target_dir.mkdir(parents=True)
        print("🛠 {name}.sql doesn't exists in cache, downloading")
        s3.client().download_file(
            Bucket=RECIPES_BUCKET,
            Key=f"datasets/{name}/{version}/{name}.sql",
            Filename=target_file_path,
        )
    return target_file_path
