"""Config presets for ingest operations"""

# Default production config
PRODUCTION_CONFIG = {
    "mode": None,
    "datasets_filter": None,
    "latest": True,
    "push": True,
    "output_csv": False,
    "overwrite_okay": True,
}

# Development/testing config - don't push to S3
DEV_CONFIG = {
    "mode": None,
    "datasets_filter": None,
    "latest": False,
    "push": False,
    "output_csv": True,
    "overwrite_okay": True,
}

# Quick test config - only process first dataset if multi-dataset source
QUICK_TEST_CONFIG = {
    "mode": None,
    "datasets_filter": None,
    "latest": False,
    "push": False,
    "output_csv": False,
    "overwrite_okay": True,
}
