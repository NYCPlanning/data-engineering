import os
from pathlib import Path

from . import (
    aws_access_key_id,
    aws_s3_bucket,
    aws_s3_endpoint,
    aws_secret_access_key,
    base_path,
    pp,
)
from .ingest import Ingestor
from .s3 import S3


class Archive:
    def __init__(self):
        """
        Initialize the Archive class instance
        """
        self.ingestor = Ingestor()
        self.s3 = S3(
            aws_access_key_id, aws_secret_access_key, aws_s3_endpoint, aws_s3_bucket
        )

    def __call__(
        self,
        path: str = None,
        output_format: str = "pgdump",
        push: bool = False,
        clean: bool = False,
        latest: bool = False,
        name: str = None,
        *args,
        **kwargs,
    ):
        """
        The `__call__` method allows a user to call a class instance with parameters.

        Parameters
        ----------
        path: path to the configutation file
        output_format: currently supported formats: `'csv'`, `'geojson'`, `'shapefile'`, `'postgres'`
        push: if `True` then push to s3
        clean: if `True`, the temporary files created under `.library` will be removed
        latest: if `True` then tag this current version we are processing to be the `latest`

        Optional Parameters
        ----------
        name: name of the dataset, if you would like to use templates already included in this package
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
        # If name specified, no template path is needed
        assert (
            path or name
        ), "Please specify either name of the dataset or path to the config file"

        _path = f"{Path(__file__).parent}/templates/{name}.yml"
        path = _path if name and os.path.isfile(_path) else path
        name = os.path.basename(path).split(".")[0]

        # Get ingestor by format
        ingestor_of_format = getattr(self.ingestor, output_format)

        # Initiate ingestion
        output_files, version, acl = ingestor_of_format(path, *args, **kwargs)

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
                if len(keys_in_latest) > 0:
                    diff_version = [
                        k
                        for k in keys_in_latest
                        if self.s3.info(k)["Metadata"].get("version", "") != version
                    ]

                    # Remove keys from the latest directory that have versions different
                    # from the version of the file currently getting added to latest
                    self.s3.rm(*diff_version)

            if clean:
                os.remove(_file)
