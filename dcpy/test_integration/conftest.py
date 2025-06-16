import os
from pathlib import Path

RESOURCES_DIR = Path(__file__).parent / "resources"
SFTP_PATH = Path(__file__).parent / "docker" / "sftp" / "remote_files"
DOCKER_FLAG = os.path.exists("/.dockerenv")
