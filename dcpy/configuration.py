from os import environ as env

CI = "CI" in env

BUILD_NAME = env.get("BUILD_NAME")

DEV_FLAG = env.get("DEV_FLAG") == "true"

RECIPES_BUCKET = env.get("RECIPES_BUCKET")
PUBLISHING_BUCKET = env.get("PUBLISHING_BUCKET")
