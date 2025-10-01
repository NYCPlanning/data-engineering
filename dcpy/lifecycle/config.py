from os import environ as env
from pathlib import Path

from dcpy import configuration


def _set_default_conf():
    ## the default is a little sparse at the moment, and a little duplicative
    ## (ie each `local_data_path` is just the `stage` name.)
    _env_data_path = env.get("DCPY_LIFECYCLE_DATA_DIR")
    data_path = (
        Path(_env_data_path)
        if _env_data_path
        else Path(__file__).parent.parent.parent / ".lifecycle"  # ugh
    )

    return {
        "product_metadata": {
            "repo_path": Path(configuration.PRODUCT_METADATA_REPO_PATH).absolute()
            if configuration.PRODUCT_METADATA_REPO_PATH
            else None
        },
        "local_data_path": data_path,
        "stages": {
            "ingest": {
                "local_data_path": "ingest",
            },
            "builds": {
                "local_data_path": "builds",
                "stages": {
                    "plan": {"local_data_path": "plan"},
                    "load": {"local_data_path": "load"},
                    "build": {"local_data_path": "build"},
                    "qa": {"local_data_path": "qa"},
                },
            },
            "package": {
                "local_data_path": "package",
                "stages": {
                    "assemble": {
                        "local_data_path": "assemble",
                        "default_connectors": {"pull": "bytes", "push": "TODO"},
                    },
                    "qa": {"local_data_path": "qa"},
                },
            },
        },
    }


# Configuration for lifecycle stages (like directories for stage data)
CONF = _set_default_conf()


def list_stages():
    stage_names = set()

    # this assumes no sub-sub-stages...
    for stage_name, stage in CONF["stages"].items():
        if "stages" in stage:
            stage_names.update([f"{stage_name}.{s}" for s in stage["stages"].keys()])
        else:
            stage_names.add(stage_name)
    return stage_names


def stage_config(stages: str):
    """Retrieve the configuration for a stage.

    stage should be a dot delmited list of stages.substages.etc, e.g. `package.qa`
    """
    curr_stage: dict = CONF
    for s in stages.split("."):
        curr_stage = curr_stage.get("stages", {}).get(s)
        if not curr_stage:
            raise Exception(f"No such stage {s} found. Full stage path: {stages}")
    return curr_stage


def local_data_path_for_stage(stages: str) -> Path:
    """Get the combination of `local_data_path`s for a stage.

    This will combine the top-level stage with each intermediate stage
    ie. `builds.plan` -> `.lifecycle/builds/plan/` for the default conf.
    """
    path = Path(CONF["local_data_path"])
    curr_stage: dict = CONF
    for s in stages.split("."):
        curr_stage = curr_stage.get("stages", {}).get(s)
        if not curr_stage:
            raise Exception(f"No such stage {s} found. Full stage path: {stages}")
        path = path / curr_stage["local_data_path"]
    return path
