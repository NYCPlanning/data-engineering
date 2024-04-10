import os


def setup_directory(dir_name: str):
    """If no existing data directory then initialize one"""
    if dir_name not in ["data/", "logs/", ".output/"]:
        raise Exception(
            "only data/ and logs/ directories should be initialized this way"
        )
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
