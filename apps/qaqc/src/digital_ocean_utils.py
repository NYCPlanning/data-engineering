import os
import requests
import json
import boto3
import pandas as pd
import geopandas as gpd
from io import BytesIO
from zipfile import ZipFile
from io import StringIO
from dotenv import load_dotenv
from src.constants import SQL_FILE_DIRECTORY, construct_dataset_by_version


construct_data_library_sql_url = lambda dataset, version: (
    f"https://edm-recipes.nyc3.cdn.digitaloceanspaces.com/datasets/{dataset}/{version}/{dataset}.sql"
)

construct_data_library_config_url = lambda dataset, version: (
    f"https://edm-recipes.nyc3.cdn.digitaloceanspaces.com/datasets/{dataset}/{version}/config.json"
)

construct_branch_output_data_directory_url = lambda dataset, branch, version: (
    f"https://edm-publishing.nyc3.digitaloceanspaces.com/{dataset}/{branch}/{version}/output"
)

construct_output_data_directory_url = lambda dataset, version: (
    f"https://edm-publishing.nyc3.digitaloceanspaces.com/{dataset}/{version}/output"
)

load_dotenv()


def get_datatset_config(dataset: str, version: str) -> dict:
    response = requests.get(
        construct_data_library_config_url(dataset=dataset, version=version), timeout=10
    )
    return json.loads(response.text)


def get_latest_build_version(dataset: str) -> str:
    # TODO handle lack of version file
    version = requests.get(
        f"{construct_output_data_directory_url(dataset=dataset, version='latest')}/version.txt",
        timeout=10,
    ).text
    return version


def get_data_library_sql_file(dataset: str, version: str) -> None:
    sql_dump_file_path_s3 = construct_data_library_sql_url(dataset, version)
    dataset_by_version = construct_dataset_by_version(dataset, version)
    sql_dump_file_path_local = SQL_FILE_DIRECTORY / f"{dataset_by_version}.sql"

    if not os.path.exists(SQL_FILE_DIRECTORY):
        os.makedirs(SQL_FILE_DIRECTORY)

    if os.path.exists(sql_dump_file_path_local):
        print(f"sql dump file already pulled : ({sql_dump_file_path_s3}")
    else:
        print(f"getting sql dump file : {sql_dump_file_path_s3} ...")
        data_mysqldump = requests.get(sql_dump_file_path_s3, timeout=300)
        with open(sql_dump_file_path_local, "wb") as f:
            f.write(data_mysqldump.content)


# NOTE this class is a legacy approach used in many reports, but prefer a functional approach
class DigitalOceanClient:
    def __init__(self, bucket_name, repo_name):
        self.bucket_name = bucket_name
        self.repo_name = repo_name

    @property
    def bucket(self):
        return self.s3_resource.Bucket(self.bucket_name)

    @property
    def bucket_is_public(self):
        return self.bucket_name == "edm-publishing"

    @property
    def repo_folder(self):
        return self.bucket.objects.filter(Prefix=f"{self.repo_name}/")

    @property
    def s3_resource(self):
        return boto3.resource(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            endpoint_url=os.getenv("AWS_S3_ENDPOINT"),
        )

    def get_all_folder_names_in_repo_folder(self, index=1):
        all_folders = set()

        for obj in self.repo_folder:
            folder = obj._key.split("/")[index]
            if folder != "":
                all_folders.add(folder)

        return all_folders

    def get_all_filenames_in_folder(self, folder_path: str):
        filenames = set()
        for object in self.bucket.objects.filter(Prefix=f"{folder_path}/"):
            filename = object.key.split("/")[-1]
            if filename != "":
                filenames.add(object.key.split("/")[-1])
        return filenames

    def unzip_csv(self, csv_filename, zipfile):
        try:
            with zipfile.open(csv_filename) as csv:
                return pd.read_csv(csv, true_values=["t"], false_values=["f"])
        except:
            return None

    def shapefile_from_DO(self, shapefile_zip):
        try:
            zip_obj = self.s3_resource.Object(
                bucket_name=self.bucket_name, key=shapefile_zip
            )
            buffer = BytesIO(zip_obj.get()["Body"].read())

            return gpd.read_file(buffer)
        except:
            raise ConnectionAbortedError(
                f"There was an issue downloading {shapefile_zip} from Digital Ocean"
            )

    def csv_from_DO(self, url, kwargs={}):
        try:
            if self.bucket_is_public:
                return self.public_csv_from_DO(url, kwargs)
            else:
                return self.private_csv_from_DO(url, kwargs)
        except:
            ConnectionAbortedError(
                f"There was an issue downloading {url} from Digital Ocean."
            )

    def public_csv_from_DO(self, url, kwargs):
        return pd.read_csv(url, **kwargs)

    def private_csv_from_DO(self, url, kwargs):
        obj = self.s3_resource.Object(bucket_name=self.bucket_name, key=url)
        s = str(obj.get()["Body"].read(), "utf8")
        data = StringIO(s)

        return pd.read_csv(data, encoding="utf8", **kwargs)

    def zip_from_DO(self, zip_filename):
        zip_obj = self.s3_resource.Object(
            bucket_name=self.bucket_name, key=zip_filename
        )
        buffer = BytesIO(zip_obj.get()["Body"].read())

        return ZipFile(buffer)
