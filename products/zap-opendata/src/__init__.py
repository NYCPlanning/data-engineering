import os

from dotenv import load_dotenv

# Load environmental variables
load_dotenv()


TENANT_ID = os.environ["TENANT_ID"]
ZAP_DOMAIN = os.environ["ZAP_DOMAIN"]
CLIENT_ID = os.environ["CLIENT_ID"]
SECRET = os.environ["SECRET"]

BUILD_ENGINE_SERVER = os.environ["BUILD_ENGINE_SERVER"]
ZAP_DB = "edm-zap"
ZAP_DB_URL = f"{BUILD_ENGINE_SERVER}/{ZAP_DB}"

base_path = ".output"
if not os.path.isdir(base_path):
    os.makedirs(base_path, exist_ok=True)
