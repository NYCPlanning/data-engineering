import os
import pprint
from pathlib import Path

# pyarrow must be imported before osgeo.gdal below: GDAL and pyarrow each bundle their
# own native shared libraries (zstd, thrift, etc), and importing GDAL first can load
# conflicting versions that break pyarrow's later import (manifests as pandas'
# to_parquet/read_parquet silently finding no usable engine). Importing pyarrow first
# makes its native libraries win. ruff's import sort would otherwise reorder this
# below osgeo, so it's kept in its own group.
import pyarrow  # noqa: F401  # isort: skip

from dotenv import load_dotenv
from osgeo import gdal
from rich.traceback import install

from dcpy import configuration

# Use rich to handle exceptions
install()

# Load environmental variables
load_dotenv()

# Initialize pretty print
pp = pprint.PrettyPrinter(indent=4)

# gdal pg config, turn off warning
gdal.SetConfigOption("PG_USE_COPY", "YES")
gdal.SetConfigOption("CPL_LOG", "/dev/null")
gdal.UseExceptions()

aws_s3_bucket = configuration.RECIPES_BUCKET

# Credentials are optional here: importing dcpy.library shouldn't fail for callers
# that never touch S3. Actual S3 operations will fail on their own if these were
# needed and never set.
gdal.SetConfigOption(
    "AWS_S3_ENDPOINT", configuration.AWS_S3_ENDPOINT.replace("https://", "")
)
if configuration.AWS_SECRET_ACCESS_KEY:
    gdal.SetConfigOption("AWS_SECRET_ACCESS_KEY", configuration.AWS_SECRET_ACCESS_KEY)
if configuration.AWS_ACCESS_KEY_ID:
    gdal.SetConfigOption("AWS_ACCESS_KEY_ID", configuration.AWS_ACCESS_KEY_ID)

# Create a local .library directory to store temporary files
base_path = ".library"
TEMPLATE_DIR = Path(__file__).parent / "templates"

if not os.path.isdir(base_path):
    os.makedirs(base_path, exist_ok=True)
    # create .gitignore so that files in this directory aren't tracked
    with open(f"{base_path}/.gitignore", "w") as f:
        f.write("*")
    os.makedirs(f"{base_path}/datasets", exist_ok=True)
    os.makedirs(f"{base_path}/configurations", exist_ok=True)

__version__ = "0.1.0"
