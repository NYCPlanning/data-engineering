import os
import pprint
from pathlib import Path

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

gdal.SetConfigOption(
    "AWS_S3_ENDPOINT", os.environ["AWS_S3_ENDPOINT"].replace("https://", "")
)
gdal.SetConfigOption("AWS_SECRET_ACCESS_KEY", os.environ["AWS_SECRET_ACCESS_KEY"])
gdal.SetConfigOption("AWS_ACCESS_KEY_ID", os.environ["AWS_ACCESS_KEY_ID"])

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
