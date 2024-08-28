from os import environ as env

_DEV_FLAG = env.get("DEV_FLAG")
if _DEV_FLAG and _DEV_FLAG not in ("true", "false"):
    raise ValueError(f"Invalid value for env var 'DEV_FLAG': '{_DEV_FLAG}'")
DEV_FLAG = _DEV_FLAG == "true"

CI = "CI" in env

BUILD_NAME = env.get("BUILD_NAME")
if DEV_FLAG and not BUILD_NAME:
    raise ValueError("'BUILD_NAME' env var needed with 'DEV_FLAG'")

RECIPES_BUCKET = "edm-recipes"
PUBLISHING_BUCKET = "edm-publishing"
DEV_BUCKET = f"de-dev-{BUILD_NAME}" if DEV_FLAG else None
