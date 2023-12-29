import os
import pprint

from dotenv import load_dotenv
from osgeo import gdal
from rich.traceback import install

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

# gdal configure aws s3 connection info
aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
aws_s3_endpoint = os.environ["AWS_S3_ENDPOINT"]
aws_s3_bucket = os.environ["AWS_S3_BUCKET"]

gdal.SetConfigOption("AWS_S3_ENDPOINT", aws_s3_endpoint.replace("https://", ""))
gdal.SetConfigOption("AWS_SECRET_ACCESS_KEY", aws_secret_access_key)
gdal.SetConfigOption("AWS_ACCESS_KEY_ID", aws_access_key_id)

# Create a local .library directory to store temporary files
base_path = ".library"

if not os.path.isdir(base_path):
    os.makedirs(base_path, exist_ok=True)
    # create .gitignore so that files in this directory aren't tracked
    with open(f"{base_path}/.gitignore", "w") as f:
        f.write("*")
    os.makedirs(f"{base_path}/datasets", exist_ok=True)
    os.makedirs(f"{base_path}/configurations", exist_ok=True)

__version__ = "0.1.0"
