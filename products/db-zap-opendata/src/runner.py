import json
import os
import sys
from pathlib import Path

import pandas as pd
from pathlib import Path
import requests
from sqlalchemy import text

from . import CLIENT_ID, SECRET, TENANT_ID, ZAP_DOMAIN, ZAP_ENGINE
from .client import Client
from .copy import psql_insert_copy
from .pg import PG
from .visible_projects import (
    OPEN_DATA,
    make_staging_table,
    make_open_data_table,
    open_data_recode,
    recode_id,
)
from .util import timestamp_to_date


class Runner:
    def __init__(self, name: str, schema: str):
        self.c = Client(
            zap_domain=ZAP_DOMAIN,
            tenant_id=TENANT_ID,
            client_id=CLIENT_ID,
            secret=SECRET,
        )
        self.name = name
        self.output_dir = f".output/{name}"
        self.cache_dir = f".cache/{name}"
        self.headers = self.c.request_header
        self.schema = schema
        self.pg = PG(ZAP_ENGINE, self.schema)
        self.engine = self.pg.engine
        self.open_dataset = self.name in OPEN_DATA

    def create_output_and_cache_directories(self):
        for directory in [self.cache_dir, self.output_dir]:
            if not os.path.isdir(directory):
                os.makedirs(directory)

    def write_to_json(self, content: str, filename: str) -> bool:
        print(f"writing {filename} ...")
        with open(f"{self.cache_dir}/{filename}", "w") as f:
            f.write(content)
        return os.path.isfile(f"{self.cache_dir}/{filename}")

    def check_table_existence(self, name):
        with self.engine.begin() as sql_conn:
            statement = """
                select * from information_schema.tables 
                where
                    table_schema='%(schema)s'
                    and table_name='%(name)s'
            """ % {
                "schema": self.pg.schema,
                "name": name,
            }
            r = sql_conn.execute(statement=text(statement))
        return bool(r.rowcount)

    def sql_to_csv(self, table_name, output_file):
        print("pd.read_sql ...")
        df = pd.read_sql(
            "select * from %(name)s" % {"name": table_name}, con=self.engine
        )
        if self.name == "dcp_projectbbls" and "timezoneruleversionnumber" in df.columns:
            # written because sql int to csv writes with decimal and big query wants int
            print("export cleaning ...")
            df["timezoneruleversionnumber"] = (
                df["timezoneruleversionnumber"]
                .str.split(".", expand=True)[0]
                .astype(int, errors="ignore")
            )

        print("df.to_csv ...")
        df.to_csv(f"{output_file}.csv", index=False)

    def download(self):
        print(f"downloading {self.name} from ZAP CRM ...")
        self.create_output_and_cache_directories()
        nextlink = f"{ZAP_DOMAIN}/api/data/v9.1/{self.name}"
        counter = 0
        while nextlink != "":
            response = requests.get(nextlink, headers=self.headers)
            result = response.text
            result_json = response.json()
            if list(result_json.keys()) == ["error"]:
                raise FileNotFoundError(result_json["error"])
            filename = f"{self.name}_{counter}.json"
            self.write_to_json(response.text, filename)
            counter += 1
            nextlink = response.json().get("@odata.nextLink", "")

    def combine(self):
        combine_table_name = f"{self.name}_crm"
        # sort file paths by file modification
        file_paths = sorted(
            Path(self.cache_dir).iterdir(),
            key=os.path.getmtime,
        )
        if self.check_table_existence(self.name):
            print("check_table_existence ...")
            with self.engine.begin() as sql_conn:
                statement = """
                    BEGIN; DROP TABLE IF EXISTS %(newname)s; 
                    ALTER TABLE %(name)s RENAME TO %(newname)s; COMMIT;
                """ % {
                    "name": combine_table_name,
                    "newname": combine_table_name + "_",
                }
                sql_conn.execute(statement=text(statement))
        else:
            print("table does not exist")

        for file_path in file_paths:
            print(f"json.load {file_path} ...")
            with open(file_path) as f:
                json_data = json.load(f)

            df = pd.DataFrame(json_data["value"], dtype=str)
            if self.name == "dcp_projects":
                df["dcp_visibility"] = df["dcp_visibility"].str.split(".", expand=True)[
                    0
                ]

            print("df.to_sql ...")
            df.to_sql(
                name=combine_table_name,
                con=self.engine,
                index=False,
                if_exists="append",
                method=psql_insert_copy,
            )

        with self.engine.begin() as sql_conn:
            statement = "BEGIN; DROP TABLE IF EXISTS %(name)s; COMMIT;" % {
                "name": combine_table_name + "_"
            }
            sql_conn.execute(statement=text(statement))

    def recode(self):
        recode_table_name = f"{self.name}_recoded"

        if not self.open_dataset:
            print(f"Nothing to recode in non-open dataset {self.name}")
            return

        make_staging_table(self.engine, self.name)

        print("pd.read_sql ...")
        source_table_name = f"{self.name}_staging"
        df = pd.read_sql(
            "select * from %(name)s"
            % {
                "name": source_table_name,
            },
            con=self.engine,
        )

        print("open_data_recode ...")
        df = open_data_recode(self.name, df, self.headers)

        if self.name == "dcp_projects":
            print("timestamp_to_date ...")
            df = timestamp_to_date(
                df,
                date_columns=[
                    "completed_date",
                    "certified_referred",
                    "current_milestone_date",
                    "current_envmilestone_date",
                    "app_filed_date",
                    "noticed_date",
                    "approval_date",
                ],
            )
            print("current_milestone_date ...")
            df.loc[
                (~df.current_milestone.isnull())
                & (df.current_milestone.str.contains("MM - Project Readiness")),
                "current_milestone_date",
            ] = None
            print("current_milestone ...")
            df.loc[
                (~df.current_milestone.isnull())
                & (df.current_milestone.str.contains("MM - Project Readiness")),
                "current_milestone",
            ] = None

            print("df.to_csv ...")
            df.to_csv(f"{self.output_dir}/{recode_table_name}.csv", index=False)

        print("df.to_sql ...")
        df.to_sql(
            name=recode_table_name,
            con=self.engine,
            index=False,
            if_exists="replace",
            method=psql_insert_copy,
        )

    def recode_id(self):
        if self.name == "dcp_projects":
            print("recode_id ...")
            recode_table_name = f"{self.name}_recoded"
            
            print("pd.read_sql ...")
            df = pd.read_sql(
                "select * from %(name)s"
                % {
                    "name": recode_table_name,
                },
                con=self.engine,
            )
            df = recode_id(df)

            print("df.to_sql ...")
            df.to_sql(
                name=recode_table_name,
                con=self.engine,
                index=False,
                if_exists="replace",
                method=psql_insert_copy,
            )
        else:
            print(f"No IDs to recode in dataset {self.name}")

    def export(self):
        output_file_internal = f"{self.output_dir}/{self.name}_internal"
        output_file_data_library = f"{self.output_dir}/{self.name}"

        if not self.open_dataset:
            source_table_name = f"{self.name}_crm"

            print(f"self.sql_to_csv for {source_table_name} ...")
            self.sql_to_csv(
                source_table_name,
                output_file_internal,
            )
            print(f"self.sql_to_csv from {source_table_name} to {output_file_data_library} ...")
            self.sql_to_csv(
                source_table_name,
                output_file_data_library,
            )
        else:
            source_table_name = f"{self.name}_recoded"

            print(f"self.sql_to_csv from {source_table_name} to {output_file_internal} ...")
            self.sql_to_csv(
                source_table_name,
                output_file_internal,
            )
            print(f"self.sql_to_csv from {source_table_name} to {output_file_data_library} ...")
            self.sql_to_csv(
                source_table_name,
                output_file_data_library,
            )

            source_table_name = f"{self.name}_visible"
            output_file_visible = f"{self.output_dir}/{self.name}_visible"

            print(f"self.sql_to_csv from {source_table_name} to {output_file_visible} ...")
            make_open_data_table(self.engine, self.name)
            self.sql_to_csv(
                source_table_name,
                output_file_visible,
            )

    def __call__(self):
        print("~~~ RUNNING download ~~~")
        self.download()
        print("~~~ RUNNING combine ~~~")
        self.combine()
        print("~~~ RUNNING recode ~~~")
        self.recode()
        print("~~~ RUNNING recode_id ~~~")
        self.recode_id()
        print("~~~ RUNNING export ~~~")
        self.export()


if __name__ == "__main__":
    name = sys.argv[1]
    schema = sys.argv[2]
    runner = Runner(name, schema)
    runner()
