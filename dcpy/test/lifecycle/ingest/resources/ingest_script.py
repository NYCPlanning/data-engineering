from pathlib import Path


def runner(create_temp_filesystem: Path):
    path = create_temp_filesystem / "t.txt"
    path.touch()
    return path
