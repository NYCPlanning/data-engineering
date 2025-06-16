import os
from pathlib import Path

RESOURCES_DIR = Path(__file__).parent / "resources"
DOCKER_FLAG = os.path.exists("/.dockerenv")
