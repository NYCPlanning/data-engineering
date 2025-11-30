from pathlib import Path

import yaml

from dcpy.utils.metadata import CIRun, RunDetails, User, get_run_details

RESOURCES_DIR = Path(__file__).parent / "resources"


def test_older_execution_details():
    with open(RESOURCES_DIR / "config_old.yml") as file:
        obj = yaml.safe_load(file)
    manual = RunDetails(**obj["manual"])
    assert manual.runner == User(username="user")
    ci = RunDetails(**obj["ci"])
    assert ci.runner == CIRun(dispatch_event="a", url="b", job="c")


def test_run_details_parsing():
    with open(RESOURCES_DIR / "config.yml") as file:
        obj = yaml.safe_load(file)
    _manual = RunDetails(**obj["manual"])
    _ci = RunDetails(**obj["ci"])
    assert True


def test_run_details_serialization():
    """Test that serialization of timestamps works as intended
    model_dump should have timestamp in string format, and then ensure that this serialized datetime
    is read back in properly by pydantic"""
    details = get_run_details()
    dumped = details.model_dump()
    assert isinstance(dumped["timestamp"], str)
    reloaded_details = RunDetails(**dumped)
    assert details.timestamp == reloaded_details.timestamp
