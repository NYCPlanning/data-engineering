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


S3_BUCKET = "edm-private"
S3_SOURCE_HOUSING_TEAM_DIR = "dcp_housing_team/db-knownprojects/20250429"
S3_OUTPUT_DIR = "db-kpdb"

DATA_PATH = _product_path / "data"
RAW_DATA_PATH = DATA_PATH / "raw"
CORRECTIONS_DATA_PATH = DATA_PATH / "corrections"
OUTPUT_PATH = _product_path / "output"

DCP_HOUSING_DATA_FILENAMES = {
    "dcp_knownprojects": "KPDB_20240710.zip",
    "esd_projects": "2021.2.10 State Developments for Housing Pipeline.xlsx",
    "edc_projects": "2022.11.18 EDC inputs for DCP housing projections.xlsx",
    "edc_dcp_inputs": "edc_shapefile_20250225.zip",
    "dcp_n_study": "nstudy_rezoning_commitments_shapefile_20221017.zip",
    "dcp_n_study_future": "future_neighborhoodstudies_20250304.zip",
    "dcp_n_study_projected": "past_neighborhoodstudies_20250304.zip",
    "hpd_rfp": "HPD_RFPs_20250110.xlsx",
    "hpd_pc": "HPD_Pipeline_20250225.xlsx",
    "dcp_planneradded": "dcp_planneradded_2025_04_07.csv",
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
VERSION = os.environ["VERSION"]
