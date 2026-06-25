import os

RECIPES_BUCKET = "test-recipes"
PUBLISHING_BUCKET = "edm-publishing"
os.environ["RECIPES_BUCKET"] = RECIPES_BUCKET
os.environ["PUBLISHING_BUCKET"] = PUBLISHING_BUCKET

# Some dcpy modules read these at import time (gdal config, github client); provide harmless
# defaults (setdefault so real values, when present, win) so importing app modules doesn't crash.
os.environ.setdefault("AWS_S3_ENDPOINT", "https://example.com")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("GHP_TOKEN", "test")
