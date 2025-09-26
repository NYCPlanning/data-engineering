import boto3
import importlib
import os
from pathlib import Path
import typer
from typing import Literal

PROD_RECIPES_BUCKET = "edm-recipes"
PROD_PUBLISHING_BUCKET = "edm-publishing"
os.environ["RECIPES_BUCKET"] = PROD_RECIPES_BUCKET
os.environ["PUBLISHING_BUCKET"] = PROD_PUBLISHING_BUCKET

from dcpy import configuration
from dcpy.models.connectors.edm.recipes import DatasetKey
from dcpy.models.connectors.edm.publishing import (
    ProductKey,
    BuildKey,
    DraftKey,
    PublishKey,
)
from dcpy.utils.s3 import get_subfolders
from dcpy.connectors.edm import recipes, publishing
from dcpy.lifecycle.builds import plan
from dcpy.lifecycle.builds import get_recipes_default_connector


ROOT_PATH = Path(__file__).parent.parent.parent
DEV_BUCKET_PREFIX = "de-dev-"
ACL: Literal["private"] = "private"
PRODUCTS = [
    "db-cbbr",
    "db-colp",
    "db-cpdb",
    "db-developments",
    "db-facilities",
    "db-green_fast_track",
    "db-template",
    "db-pluto",
    "db-zoningtaxlots",
]


def copy_folder_to_dev_bucket(source_bucket, target_bucket, source_path, target_path):
    """
    Thin wrapper over s3.copy_folder
    Ensures that bucket being copied to is a dev bucket
    """
    from dcpy.utils import s3

    assert target_bucket.startswith(DEV_BUCKET_PREFIX)
    s3.copy_folder(
        bucket=source_bucket,
        target_bucket=target_bucket,
        source_path=source_path,
        target_path=target_path,
        acl=ACL,
    )


def resolve_latest_recipe(
    target_bucket: str,
    ds: str,
):
    """
    Within a dev bucket, resolves/copies the dataset currently in "latest"
    into its versioned folder under "datasets/{dataset_id}"
    """
    os.environ["RECIPES_BUCKET"] = target_bucket
    importlib.reload(configuration)
    assert configuration.RECIPES_BUCKET != PROD_RECIPES_BUCKET
    input = recipes.Dataset(id=ds, version="latest")

    version = get_recipes_default_connector().get_latest_version(ds)
    resolved = recipes.Dataset(id=ds, version=version)
    os.environ["RECIPES_BUCKET"] = PROD_RECIPES_BUCKET
    importlib.reload(configuration)
    assert target_bucket.startswith(DEV_BUCKET_PREFIX)
    copy_folder_to_dev_bucket(
        source_bucket=target_bucket,
        target_bucket=target_bucket,
        source_path=recipes.s3_folder_path(input) + "/",
        target_path=recipes.s3_folder_path(resolved) + "/",
    )


def resolve_latest_publish(target_bucket: str, data_product: str):
    """
    Within a dev bucket, resolves/copies the data product currently in "publish/latest"
    into its versioned folder within "{product}/publish"
    """
    os.environ["PUBLISHING_BUCKET"] = target_bucket
    importlib.reload(configuration)
    assert configuration.PUBLISHING_BUCKET != PROD_PUBLISHING_BUCKET
    input = PublishKey(product=data_product, version="latest")
    latest = publishing.get_latest_version(data_product)
    assert latest
    resolved = PublishKey(product=data_product, version=latest)
    os.environ["PUBLISHING_BUCKET"] = PROD_PUBLISHING_BUCKET
    importlib.reload(configuration)
    copy_folder_to_dev_bucket(
        source_bucket=target_bucket,
        target_bucket=target_bucket,
        source_path=input.path + "/",
        target_path=resolved.path + "/",
    )


def clone_recipe(
    target_bucket: str,
    dataset_id: str,
    version: str = "latest",
):
    """
    Given a dataset id, copy it from edm-recipes
    If provided version is "latest", also resolve it to its versioned folder
    """
    key = DatasetKey(id=dataset_id, version=version)
    copy_folder_to_dev_bucket(
        source_bucket=PROD_RECIPES_BUCKET,
        target_bucket=target_bucket,
        source_path=f"{recipes.s3_folder_path(key)}/",
        target_path=f"{recipes.s3_folder_path(key)}/",
    )
    if version == "latest":
        resolve_latest_recipe(target_bucket, dataset_id)


def clone_recipes_latest(
    target_bucket: str,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
):
    """
    Clone latest versions of various recipe datasets from production bucket to dev bucket
    By default, targets all datasets
    Specific datasets can be excluded or included with `include` or 'exclude_dataset`
    If `include` is specified, only the specified datasets are cloned
    """
    if include and exclude:
        raise ValueError("Only one of `include` and `exclude` may be provided")
    datasets = get_subfolders(bucket=PROD_RECIPES_BUCKET, prefix="datasets/")
    if include:
        datasets = include
    if exclude:
        datasets = [ds for ds in datasets if ds not in exclude]

    for ds in datasets:
        clone_recipe(target_bucket, ds)


def clone_data_products(
    target_bucket: str,
    data_products: list[str],
    include_recipe_datasets: bool = True,
):
    """
    Clone latest published version of each provided data product
    If `include_recipe_datasets`, input datasets for all provided products are collected and then cloned (latest versions only)

    This is a good target if setting up a dev bucket to work on enhancements for a data product
    """
    if include_recipe_datasets:
        recipe_datasets = []
        for product in data_products:
            recipe = plan.recipe_from_yaml(
                ROOT_PATH / "products" / product.strip("db-") / "recipe.yml"
            )
            recipe_datasets += [ds.id for ds in recipe.inputs.datasets]
        for ds in set(recipe_datasets):
            clone_recipe(target_bucket, ds)

    for product in data_products:
        key = PublishKey(product=product, version="latest")
        copy_folder_to_dev_bucket(
            source_bucket=PROD_PUBLISHING_BUCKET,
            target_bucket=target_bucket,
            source_path=key.path + "/",
            target_path=key.path + "/",
        )

        resolve_latest_publish(target_bucket, product)


def clone_data_product_by_key(
    target_bucket: str,
    key: ProductKey,
    include_recipe_datasets: bool = False,
):
    """
    Clone specific product key and optionally the input datasets that went into it

    This is a good target if working on an enhancement that involves rebuilding a specific product instance
    """
    copy_folder_to_dev_bucket(
        source_bucket=PROD_PUBLISHING_BUCKET,
        target_bucket=target_bucket,
        source_path=key.path + "/",
        target_path=key.path + "/",
    )
    if include_recipe_datasets:
        for index, row in publishing.get_source_data_versions(key).iterrows():
            dataset_id = str(index)
            clone_recipe(target_bucket, dataset_id, row["version"])


def setup(id: str, clean: bool = False) -> str:
    """
    Sets up a dev bucket
    Creates bucket if it doesn't exist.
    If clean flag provided, deletes all objects within bucket
    """
    from dcpy.utils import s3

    bucket = f"{DEV_BUCKET_PREFIX}{id}"
    if bucket not in [
        bucket["Name"] for bucket in s3.client().list_buckets()["Buckets"]
    ]:
        s3.client().create_bucket(Bucket=bucket, ACL=ACL)
    else:
        if not clean:
            raise Exception(
                f"bucket {bucket} already exists. specify '-c' to clean contents of bucket"
            )
        aws_s3_endpoint = os.environ.get("AWS_S3_ENDPOINT")
        aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
        aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
        assert bucket.startswith(DEV_BUCKET_PREFIX)
        bucket_obj = boto3.resource(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=aws_s3_endpoint,
        ).Bucket(bucket)
        assert bucket_obj.name.startswith(DEV_BUCKET_PREFIX)
        bucket_obj.objects.all().delete()
    return bucket


app = typer.Typer()


@app.command("recipe_single")
def _recipe_single(
    target_bucket_id: str = typer.Option(None, "--bucket-id"),
    dataset_id: str = typer.Option(None, "--dataset", "-d"),
    version: str = typer.Option(None, "--version", "-v"),
):
    clone_recipe(f"{DEV_BUCKET_PREFIX}{target_bucket_id}", dataset_id, version)


@app.command("recipes_all")
def _recipes(
    target_bucket_id: str = typer.Option(None, "--bucket-id"),
    include: list[str] | None = typer.Option(None, "--include", "-i"),
    exclude: list[str] | None = typer.Option(None, "--exclude", "-e"),
):
    clone_recipes_latest(f"{DEV_BUCKET_PREFIX}{target_bucket_id}", include, exclude)


@app.command("product")
def _publishing_single(
    target_bucket_id: str = typer.Option(None, "--bucket-id"),
    product: str = typer.Option(None, "--product", "-p"),
):
    clone_data_products(f"{DEV_BUCKET_PREFIX}{target_bucket_id}", [product])


@app.command("publishing_build_key")
def _build_key(
    target_bucket_id: str = typer.Option(None, "--bucket-id"),
    product: str = typer.Option(None, "--product", "-p"),
    build: str = typer.Option(None, "--build", "-b"),
    include_sources: bool = typer.Option(False, "--include-sources"),
):
    clone_data_product_by_key(
        f"{DEV_BUCKET_PREFIX}{target_bucket_id}",
        BuildKey(product=product, build=build),
        include_recipe_datasets=include_sources,
    )


@app.command("publishing_draft_key")
def _draft_key(
    target_bucket_id: str = typer.Option(None, "--bucket-id"),
    product: str = typer.Option(None, "--product", "-p"),
    version: str = typer.Option(None, "--version", "-v"),
    revision: str = typer.Option(None, "--revision", "-r"),
    include_sources: bool = typer.Option(False, "--include-sources"),
):
    clone_data_product_by_key(
        f"{DEV_BUCKET_PREFIX}{target_bucket_id}",
        DraftKey(product=product, version=version, revision=revision),
        include_recipe_datasets=include_sources,
    )


@app.command("publishing_publish_key")
def _publish_key(
    target_bucket_id: str = typer.Option(None, "--bucket-id"),
    product: str = typer.Option(None, "--product", "-p"),
    version: str = typer.Option(None, "--version", "-v"),
    include_sources: bool = typer.Option(False, "--include-sources"),
):
    clone_data_product_by_key(
        f"{DEV_BUCKET_PREFIX}{target_bucket_id}",
        PublishKey(product=product, version=version),
        include_recipe_datasets=include_sources,
    )


@app.command("publishing_all")
def _publish(
    target_bucket_id: str = typer.Option(None, "--bucket-id"),
):
    clone_data_products(
        f"{DEV_BUCKET_PREFIX}{target_bucket_id}",
        PRODUCTS,
        include_recipe_datasets=False,
    )


@app.command("setup")
def _setup(
    target_bucket_id: str = typer.Argument(),
    clean: bool = typer.Option(False, "--clean", "-c"),
):
    setup(target_bucket_id, clean)


@app.command("standard")
def _standard(
    target_bucket_id: str = typer.Argument(),
    products: list[str] = typer.Option(None, "--product", "-p"),
):
    assert products, "At least one product must be provided"
    assert set(products).issubset(set(PRODUCTS))

    clone_data_products(
        f"{DEV_BUCKET_PREFIX}{target_bucket_id}",
        products,
        include_recipe_datasets=True,
    )


if __name__ == "__main__":
    app()
