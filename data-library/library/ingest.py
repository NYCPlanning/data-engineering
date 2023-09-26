import os
import pathlib
import shutil
import zipfile
import datetime
from functools import wraps
from typing import Optional, Tuple
from math import floor

from osgeo import gdal
from pathlib import Path
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)

from . import base_path
from .config import Config
from .sources import generic_source, postgres_source


def translator(func):
    @wraps(func)
    def wrapper(self: "Ingestor", *args, **kwargs) -> Tuple[list[str], str, str]:
        # get relevant translator return values
        (dstDS, output_format, output_suffix, compress, inplace) = func(
            self, *args, **kwargs
        )

        output_files = []
        path = args[0]
        c = Config(path, kwargs.get("version", None))
        dataset, source, destination, _ = c.compute_parsed
        name = dataset["name"]
        version = dataset["version"]
        acl = dataset["acl"]
        # initiate source and destination datasets
        folder_path = f"{self.base_path}/datasets/{name}/{version}"

        if not output_suffix and not output_format:
            output_suffix = pathlib.Path(source["url"]["gdalpath"]).suffix.strip(".")

        if output_suffix:
            destination_path = f"{folder_path}/{name}.{output_suffix}"
            output_files.append(destination_path)
        else:
            destination_path = None

        # Create output folder and output config
        if folder_path and output_suffix:
            os.makedirs(folder_path, exist_ok=True)
            self.write_config(f"{folder_path}/config.json", c.compute_json)
            self.write_config(f"{folder_path}/config.yml", c.compute_yml)
            output_files.append(f"{folder_path}/config.json")
            output_files.append(f"{folder_path}/config.yml")

        if not output_format:
            if not destination_path:
                raise Exception("TODO - fix logic around as-is output")
            else:
                shutil.copy(source["url"]["gdalpath"], destination_path)
                return output_files, version, acl

        # Default dstDS is destination_path if no dstDS is specificed
        dstDS = destination_path if not dstDS else dstDS
        srcDS = generic_source(
            path=source["url"]["gdalpath"],
            options=source["options"],
            fields=destination["fields"],
        )

        layerName = srcDS.GetLayer(0).GetName()
        dataset_name = destination["name"]
        sql = destination.get("sql", None)
        sql = None if not sql else sql.replace("@filename", layerName)

        # Create postgres database schema and table version if needed
        if output_format == "PostgreSQL":
            schema_name = dataset_name
            dstDS.ExecuteSQL(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
            version = (
                datetime.date.today().strftime("%Y/%m/%d")
                if version == ""
                else version.lower()
            )
            layerName = f"{schema_name}.{version}"
        else:
            layerName = dataset_name

        # Initiate vector translate
        with Progress(
            SpinnerColumn(spinner_name="earth"),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=30),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            transient=True,
        ) as progress:
            task = progress.add_task(
                f"[green]Ingesting [bold]{dataset_name}[/bold]", total=1000
            )

            def update_progress(complete, message, unknown):
                progress.update(task, completed=floor(complete * 1000))

            # This addresses a gdal issue where translation will fail
            # to generate a csv from a shapefile if a csv already exists at
            # the target path
            if type(dstDS) == str and dstDS.endswith(".csv") and Path(dstDS).exists():
                Path(dstDS).unlink()

            gdal.VectorTranslate(
                dstDS,
                srcDS,
                format=output_format,
                layerCreationOptions=destination["options"],
                dstSRS=destination["geometry"]["SRS"],
                srcSRS=source["geometry"]["SRS"],
                geometryType=destination["geometry"]["type"],
                layerName=layerName,
                accessMode="overwrite",
                makeValid=True,
                # optional settings
                SQLStatement=sql,
                SQLDialect="SQLite",
                callback=update_progress,
            )

        # Create latest view in postgres database if needed
        if output_format == "PostgreSQL":
            dstDS.ExecuteSQL(f"DROP VIEW IF EXISTS {schema_name}.latest;")
            dstDS.ExecuteSQL(f"DROP TABLE IF EXISTS {schema_name}.latest;")
            dstDS.ExecuteSQL(
                f"""
                CREATE VIEW {schema_name}.latest
                as (SELECT \'{version}\' as v, *
                from {schema_name}."{version}");
                """
            )

        # Compression if needed
        if compress and destination_path:
            if output_format == "ESRI Shapefile":
                files = [
                    f"{destination_path[:-4]}.{suffix}"
                    for suffix in ["shp", "prj", "shx", "dbf"]
                ]
                self.compress(f"{destination_path}.zip", *files, inplace=True)
                output_files.remove(destination_path)
                output_files.append(f"{destination_path}.zip")
            else:
                self.compress(
                    f"{destination_path}.zip", destination_path, inplace=inplace
                )
                if inplace:
                    output_files.remove(destination_path)
                output_files.append(f"{destination_path}.zip")
        return output_files, version, acl

    return wrapper


class Ingestor:
    def __init__(self):
        self.base_path = base_path

    def compress(self, path: str, *files, inplace: bool = True):
        with zipfile.ZipFile(
            path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
        ) as _zip:
            for f in files:
                if os.path.isfile(f):
                    _zip.write(f, os.path.basename(f))
                    if inplace:
                        os.remove(f)
                else:
                    print(f"{f} does not exist!")
        return True

    def write_config(self, path: str, config: str):
        with open(path, "w") as f:
            f.write(config)

    @translator
    def postgres(
        self,
        path: str,
        compress: bool = False,
        inplace: bool = False,
        postgres_url: Optional[str] = None,
        *args,
        **kwargs,
    ):
        """
        https://gdal.org/drivers/vector/pg.html

        This function will take in a configuration then send to a
        postgres database
        path: path of the configuration
        postgres_url: connection string for the destination database
        compress: default to False because no files created when output to "PostgreSQL"
        inplace: default to False because no compress = False by default
        """
        if postgres_url is None:  # Done here to adhere to translator format
            raise ValueError("postgres_url must not be None")
        else:
            dstDS = postgres_source(postgres_url)
            return dstDS, "PostgreSQL", None, compress, inplace

    @translator
    def csv(
        self, path: str, compress: bool = False, inplace: bool = False, *args, **kwargs
    ):
        """
        https://gdal.org/drivers/vector/csv.html

        path: path of the configuration file
        compress: True if compression is needed
        inplace: True if the compressed file will replace the original output
        """
        return None, "CSV", "csv", compress, inplace

    @translator
    def pgdump(
        self, path: str, compress: bool = False, inplace: bool = False, *args, **kwargs
    ):
        """
        https://gdal.org/drivers/vector/pgdump.html

        path: path of the configuration file
        compress: True if compression is needed
        inplace: True if the compressed file will replace the original output
        """
        return None, "PGDump", "sql", compress, inplace

    @translator
    def shapefile(
        self, path: str, compress: bool = True, inplace: bool = True, *args, **kwargs
    ):
        """
        https://gdal.org/drivers/vector/shapefile.html

        path: path of the configuration file
        compress: default to True so that [shp, shx, dbf, prj] are bundled
        inplace: default to True for ease of transport
        """
        return None, "ESRI Shapefile", "shp", compress, inplace

    @translator
    def geojson(
        self, path: str, compress: bool = False, inplace: bool = False, *args, **kwargs
    ):
        """
        https://gdal.org/drivers/vector/geojson.html

        path: path of the configuration file
        compress: True if compression is needed
        inplace: True if the compressed file will replace the original output
        """
        return None, "GeoJSON", "geojson", compress, inplace

    @translator
    def as_is(
        self, path: str, compress: bool = False, inplace: bool = False, *args, **kwargs
    ):
        return None, None, None, compress, inplace
