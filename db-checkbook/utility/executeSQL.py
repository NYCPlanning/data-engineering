import os

from . import BUILD_ENGINE


def ExecuteSQL(script):
    cmd = f"psql {BUILD_ENGINE} -v ON_ERROR_STOP=1 -f {script}"
    if os.system(cmd) != 0:
        raise Exception(f"{script} has errors!")
    return None