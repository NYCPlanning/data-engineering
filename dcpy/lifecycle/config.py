from pathlib import Path


def _set_default_conf():
    return {
        "local_data_path": ".lifecycle",
        "stages": {
            "ingest": {
                "local_data_path": "ingest",
            },
            "builds": {
                "local_data_path": "builds",
                "stages": {
                    "plan": {"local_data_path": "plan"},
                    "load": {"local_data_path": "load"},
                },
            },
            "package": {
                "local_data_path": "package",
                "stages": {
                    "assemble": {"local_data_path": "assemble"},
                    "qa": {"local_data_path": "qa"},
                },
            },
        },
    }


CONF = _set_default_conf()


def stage_conf(stages: str):
    curr_stage: dict = CONF
    for s in stages.split("."):
        curr_stage = curr_stage.get("stages", {}).get(s)
        if not curr_stage:
            raise Exception(f"No such stage {s} found. Full stage path: {stages}")
    return curr_stage


def stage_path(stages: str):
    path = Path(CONF["local_data_path"])
    curr_stage: dict = CONF
    for s in stages.split("."):
        curr_stage = curr_stage.get("stages", {}).get(s)
        if not curr_stage:
            raise Exception(f"No such stage {s} found. Full stage path: {stages}")
        path = path / curr_stage["local_data_path"]
    return path
