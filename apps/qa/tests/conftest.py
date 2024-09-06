import os

RECIPES_BUCKET = "test-recipes"
PUBLISHING_BUCKET = "edm-publishing"
os.environ["RECIPES_BUCKET"] = RECIPES_BUCKET
os.environ["PUBLISHING_BUCKET"] = PUBLISHING_BUCKET
