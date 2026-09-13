"""Push PLUTO's BBL/council extract to DOF's Axway SecureTransport folder.

DOF pulls this from the same server we pull their PTS and CAMA inputs from. It
doesn't go through the metadata-driven distribute stage, which expects a dataset
package: this is one csv that DOF's scripts read by name.
"""

from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZipFile

import typer

from dcpy.connectors.edm import publishing
from dcpy.connectors.sftp import SFTPConnector
from dcpy.lifecycle.connector_registry import connectors
from dcpy.utils.logging import logger

PRODUCT = "db-pluto"
PUBLISHED_ZIP = "dof/pluto.zip"
FILENAME = "pluto.csv"
DEFAULT_FOLDER = "ToDOF"

app = typer.Typer(add_completion=False)


@app.command("run")
def run(
    version: str = typer.Argument(help="Published PLUTO version, e.g. 26v2"),
    folder: str = typer.Option(DEFAULT_FOLDER, help="Destination folder on Axway"),
    dry_run: bool = typer.Option(False, help="Download and unpack, but don't push"),
) -> None:
    axway = connectors["axway", SFTPConnector]

    # This is a shared server we've only ever read from, so name the folders we
    # can see before writing rather than trusting the one we were told.
    parent = str(Path(folder).parent)
    subfolders = axway.get_subfolders(parent)
    logger.info(f"Folders under '{parent}' on Axway: {subfolders}")
    if Path(folder).name not in subfolders:
        raise ValueError(f"No folder '{folder}' on the Axway server")

    key = f"{folder}/{FILENAME}"
    with TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        zip_path = publishing.download_file(
            publishing.PublishKey(PRODUCT, version), PUBLISHED_ZIP, tmp_path
        )
        # version.txt rides along in the published zip for our own archive; DOF
        # asked for the csv alone.
        with ZipFile(zip_path) as z:
            csv_path = Path(z.extract(FILENAME, tmp_path))

        if dry_run:
            logger.info(f"Dry run: would push {csv_path.stat().st_size} bytes to {key}")
            return

        axway.push(key=key, filepath=csv_path)
        logger.info(f"Pushed PLUTO {version} {FILENAME} to {key}")
