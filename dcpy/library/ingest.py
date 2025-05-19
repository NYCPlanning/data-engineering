from __future__ import annotations
import os
import pathlib
import shutil
import zipfile
import datetime
from functools import wraps
from math import floor
import json
import yaml

from osgeo import gdal
from pathlib import Path
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)

from dcpy.utils.metadata import get_run_details
from dcpy.models import library

from . import base_path
from .config import Config
from .sources import generic_source, postgres_source


def format_field_names(
    dataset: gdal.Dataset,
    fields: list[str] | None,
    sql: str | None,
    has_geom: bool,
    output_format: str,
):
    layer = dataset.GetLayer(0)
    layer_defn = layer.GetLayerDefn()
    layer_name = layer.GetName()

    field_mapping: dict[str, str] = {}
    if fields:
        for i in range(len(fields)):
            fieldDefn = layer_defn.GetFieldDefn(i)
            field_mapping[fieldDefn.GetName()] = fields[i]
    else:
        for i in range(layer_defn.GetFieldCount()):
            fieldDefn = layer_defn.GetFieldDefn(i)
            print(fieldDefn.__dir__())
            print(fieldDefn.GetName())
            print(fieldDefn.GetType())
            fieldName = fieldDefn.GetName()
            field_mapping[fieldName] = fieldName.replace(" ", "_").lower()
    select = ",\n\t".join([f"{old} AS {field_mapping[old]}" for old in field_mapping])
    if has_geom:
        geom_columns = {"csv": "WKT", "pg_dump": "wkb_geometry", "parquet": "geom"}
        geom_column = geom_columns.get(output_format, "geom")
        geom_clause = f',\n\tGeometry AS "{geom_column}"'
    else:
        geom_clause = ""
    query = f"""SELECT \n\t{select}{geom_clause} FROM {layer_name}"""
    if not sql:
        return query
    else:
        cte_name = "__cte__"
        sql = sql.replace("@filename", cte_name)
        if sql.startswith("WITH "):
            sql = sql.strip("WITH ")
            return f"WITH {cte_name} AS ({select}), {sql}"
        else:
            return f"WITH {cte_name} AS ({select}) \n{sql}"


def translator(func):
    @wraps(func)
    def wrapper(self: Ingestor, *args, **kwargs) -> tuple[list[str], library.Config]:
        # get relevant translator return values
        (dstDS, output_format, output_suffix, compress, inplace) = func(
            self, *args, **kwargs
        )

        output_files = []
        path = args[0]
        c = Config(path, kwargs.get("version"), kwargs.get("source_path_override"))
        dataset = c.compute
        assert dataset.source.gdalpath
        assert dataset.version
        # initiate source and destination datasets
        folder_path = f"{self.base_path}/datasets/{dataset.name}/{dataset.version}"

        if not output_suffix and not output_format:
            output_suffix = pathlib.Path(dataset.source.gdalpath).suffix.strip(".")

        if output_suffix:
            destination_path = f"{folder_path}/{dataset.name}.{output_suffix}"
            output_files.append(destination_path)
        else:
            destination_path = None

        execution_details = get_run_details()

        # Create output folder and output config
        if folder_path and output_suffix:
            execution_details.logged = True
            os.makedirs(folder_path, exist_ok=True)
            config_dumped = library.Config(
                dataset=dataset,
                execution_details=execution_details,
            ).model_dump(mode="json")
            with open(f"{folder_path}/config.json", "w") as f:
                f.write(json.dumps(config_dumped, indent=4))
            output_files.append(f"{folder_path}/config.json")
            with open(f"{folder_path}/config.yml", "w") as f:
                yaml.dump(config_dumped, f)
            output_files.append(f"{folder_path}/config.yml")

        config = library.Config(
            dataset=dataset,
            execution_details=execution_details,
        )

        if not output_format:
            if not destination_path:
                raise Exception("TODO - fix logic around as-is output")
            else:
                shutil.copy(dataset.source.gdalpath, destination_path)
                return output_files, config

        # Default dstDS is destination_path if no dstDS is specificed
        dstDS = destination_path if not dstDS else dstDS
        srcDS = generic_source(
            path=dataset.source.gdalpath, options=dataset.source.options
        )

        if srcDS.GetLayerCount() > 1:
            if dataset.source.layer_name:
                src_layer = dataset.source.layer_name
            else:
                raise Exception(
                    "Multiple layers in source dataset found, must specify one in recipe."
                )
        else:
            src_layer = srcDS.GetLayer(0).GetName()

        sql = format_field_names(
            srcDS,
            dataset.destination.fields,
            dataset.destination.sql,
            dataset.source.geometry is not None,
            output_format,
        )

        # Create postgres database schema and table version if needed
        if output_format == "PostgreSQL":
            schema_name = dataset.name
            dstDS.ExecuteSQL(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
            version = (
                datetime.date.today().strftime("%Y/%m/%d")
                if dataset.version == ""
                else dataset.version.lower()
            )
            layerName = f"{schema_name}.{version}"
        else:
            layerName = dataset.name

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
                f"[green]Ingesting [bold]{dataset.name}[/bold]", total=1000
            )

            def update_progress(complete, message, unknown):
                progress.update(task, completed=floor(complete * 1000))

            # This addresses a gdal issue where translation will fail
            # to generate a csv from a shapefile if a csv already exists at
            # the target path

            if output_format in ("CSV", "Parquet") and Path(dstDS).exists():
                Path(dstDS).unlink()

            srcSRS = dataset.source.geometry.SRS if dataset.source.geometry else None

            gdal.VectorTranslate(
                dstDS,
                srcDS,
                format=output_format,
                layerCreationOptions=dataset.destination.options,
                dstSRS=dataset.destination.geometry.SRS,
                srcSRS=srcSRS,
                geometryType=dataset.destination.geometry.type,
                layers=[src_layer],
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
        return output_files, config

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

    @translator
    def postgres(
        self,
        path: str,
        compress: bool = False,
        inplace: bool = False,
        postgres_url: str | None = None,
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
    def parquet(
        self, path: str, compress: bool = False, inplace: bool = False, *args, **kwargs
    ):
        """
        https://gdal.org/drivers/vector/parquet.html

        path: path of the configuration file
        compress: True if compression is needed
        inplace: True if the compressed file will replace the original output
        """
        return None, "Parquet", "parquet", compress, inplace

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
