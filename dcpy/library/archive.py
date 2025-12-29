import os
from pathlib import Path
from typing import Any, Callable

from dcpy.models.library import Config
from . import base_path
from .ingest import Ingestor
from .s3 import S3


class Archive:
    def __init__(self):
        """
        Initialize the Archive class instance
        """
        self.ingestor = Ingestor()
        self.s3 = S3()

    def __call__(
        self,
        path: str | None = None,
        output_format: str = "pgdump",
        push: bool = False,
        clean: bool = False,
        latest: bool = False,
        name: str | None = None,
        source_path_override: str | None = None,
        *args,
        **kwargs,
    ) -> Config:
        """
        The `__call__` method allows a user to call a class instance with parameters.

        Parameters
        ----------
        path: path to the configutation file
        output_format: see ingest.Ingestor translator methods for currently supported formats`
        push: if `True` then push to s3
        clean: if `True`, the temporary files created under `.library` will be removed
        latest: if `True` then tag this current version we are processing to be the `latest`
        name: name of the dataset, if you would like to use templates already included in this package
        source_path_override: if applicable, a str url/path to override the source path in the template

        Optional Parameters
        ----------
        compress: if compression is needed, this is passed into the `ingestor`
        inplace: if compressed zip file will replace the original file, this is passed into the `ingestor`
        postgres_url: Please specify if `output_format=='postgres'`
        version: specify version if using a custom version name

        Sample Usage
        ----------
        ### A flat file example:
        ```python
        from library.archive import Archive
        a = Archive()
        a(
            "path/to/config.yml",
            output_format="csv", push=True, clean=True,
            latest=True, compress=True,
        )
        ```
        ### A postgres example:
        > by default, for postgres `push`, `clean`, `latest`, `compress` are all default to `False`
        ```python
        postgres_url='postgresql://user:password@hose/db'
        a(
            "path/to/config.yml",
            output_format="postgres",
            postgres_url=postgres_url,
        )
        ```
        """

        if path:
            name = name or os.path.basename(path).split(".")[0]
        elif name:
            path = f"{Path(__file__).parent}/templates/{name}.yml"
        else:
            raise Exception(
                "Please specify either name of the dataset or path to the config file"
            )

        if not os.path.isfile(path):
            raise FileNotFoundError(
                f"Template file '{path}' not found. Try providing path explicitly if you've only provided name"
            )

        # Get ingestor by format
        ingestor_of_format: Callable[[Any], tuple[list[str], Config]] = getattr(
            self.ingestor, output_format
        )

        # Initiate ingestion
        output_files, config = ingestor_of_format(
            path,
            *args,
            source_path_override=source_path_override,
            **kwargs,  # type: ignore
        )
        version = config.dataset.version
        acl = config.dataset.acl

        # Write to s3
        for _file in output_files:
            if push:
                key = _file.replace(base_path + "/", "")
                self.s3.put(_file, key, acl, metadata={"version": version})
            if push and latest:
                # Upload file to a latest directory, where version metadata is version
                # This allows us to get the version associated with each file in latest
                self.s3.put(
                    _file,
                    key.replace(version, "latest"),
                    acl,
                    metadata={"version": version},
                )

                # Find all files in latest where the version (stored in s3 metadata)
                # does not match the version of the file currently getting added to latest
                keys_in_latest = self.s3.ls(f"datasets/{name}/latest")
                diff_version = [
                    k
                    for k in keys_in_latest
                    if self.s3.info(k)["Metadata"].get("version", "") != version
                ]

                # Remove keys from the latest directory that have versions different
                # from the version of the file currently getting added to latest
                if len(diff_version) > 0:
                    self.s3.rm(*diff_version)

            if clean:
                os.remove(_file)

        return config
