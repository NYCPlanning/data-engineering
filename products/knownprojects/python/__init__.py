import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

_module_top_path = Path(__file__).resolve().parent
_product_path = _module_top_path.parent
_proj_root = _product_path.parent.parent

# Make `dcpy` available
sys.path.append(str(_proj_root))

DATE = datetime.today().strftime("%Y-%m-%d")

S3_BUCKET = "edm-private"
S3_SOURCE_HOUSING_TEAM_DIR = "dcp_housing_team/db-knownprojects/20230725"
S3_OUTPUT_DIR = "db-kpdb"

DATA_PATH = _product_path / "data"
RAW_DATA_PATH = DATA_PATH / "raw"
CORRECTIONS_DATA_PATH = DATA_PATH / "corrections"
OUTPUT_PATH = _product_path / "output"

DCP_HOUSING_DATA_FILENAMES = {
    "dcp_knownprojects": "kpdb_20211006_shapefiles.zip",
    "esd_projects": "2021.2.10 State Developments for Housing Pipeline.xlsx",
    "edc_projects": "2022.11.18 EDC inputs for DCP housing projections.xlsx",
    "edc_dcp_inputs": "edc_shapefile_20221118.zip",
    "dcp_n_study": "nstudy_rezoning_commitments_shapefile_20221017.zip",
    "dcp_n_study_future": "future_nstudy_shapefile_20221017.zip",
    "dcp_n_study_projected": "PastNeighborhoodStudies_20221019.zip",
    "hpd_rfp": "20221122_HPD_RFPs.xlsx",
    "hpd_pc": "2022_11_23 DCP_SCA Pipeline.xlsx",
    "dcp_planneradded": "dcp_planneradded_2023_03_21.csv",
}
DCP_HOUSING_CORRECTIONS_FILENAMES = {
    "corrections_dob": "corrections_dob.csv",
    "corrections_main": "corrections_main.csv",
    "corrections_project": "corrections_project.csv",
    "zap_record_ids": "zap_record_ids.csv",
}

# Load environmental variables
load_dotenv()
BUILD_ENGINE = os.environ["BUILD_ENGINE"]
BUILD_ENGINE_SCHEMA = os.environ["BUILD_ENGINE_SCHEMA"]
