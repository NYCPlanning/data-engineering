import os

from dotenv import load_dotenv

# Load environmental variables
load_dotenv()


TENANT_ID = os.environ["TENANT_ID"]
ZAP_DOMAIN = os.environ["ZAP_DOMAIN"]
CLIENT_ID = os.environ["CLIENT_ID"]
SECRET = os.environ["SECRET"]
ZAP_ENGINE = os.environ["ZAP_ENGINE"]

base_path = ".output"
if not os.path.isdir(base_path):
    os.makedirs(base_path, exist_ok=True)
