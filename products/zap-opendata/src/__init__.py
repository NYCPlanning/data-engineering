import os

from dotenv import load_dotenv

# Load environmental variables
load_dotenv()


# Not required at import: the tests exercise pure logic and shouldn't need CRM or
# database credentials to load this package. A missing value surfaces on first use.
TENANT_ID = os.environ.get("TENANT_ID")
ZAP_DOMAIN = os.environ.get("ZAP_DOMAIN")
CLIENT_ID = os.environ.get("CLIENT_ID")
SECRET = os.environ.get("SECRET")

BUILD_ENGINE_SERVER = os.environ.get("BUILD_ENGINE_SERVER")
ZAP_DB = "edm-zap"
ZAP_DB_URL = f"{BUILD_ENGINE_SERVER}/{ZAP_DB}" if BUILD_ENGINE_SERVER else None

base_path = ".output"
if not os.path.isdir(base_path):
    os.makedirs(base_path, exist_ok=True)
