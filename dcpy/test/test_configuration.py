import importlib
import os
import pytest
from dcpy import configuration


class TestDevFlag:
    @pytest.fixture(autouse=True)
    def setup(self):
        os.environ["BUILD_NAME"] = "test_configuration"
        yield
        for s in ("DEV_FLAG", "BUILD_NAME"):
            if s in os.environ:
                os.environ.pop(s)

    def test_no_value(self):
        assert not configuration.DEV_FLAG

    def test_true(self):
        os.environ["DEV_FLAG"] = "true"
        importlib.reload(configuration)
        assert configuration.DEV_FLAG

    def test_false(self):
        os.environ["DEV_FLAG"] = "false"
        importlib.reload(configuration)
        assert not configuration.DEV_FLAG

    def test_invalid(self):
        os.environ["DEV_FLAG"] = "invalid"
        with pytest.raises(ValueError, match="Invalid value for env var 'DEV_FLAG'"):
            importlib.reload(configuration)

    def test_no_build_name(self):
        os.environ.pop("BUILD_NAME")
        os.environ["DEV_FLAG"] = "true"
        with pytest.raises(
            ValueError, match="'BUILD_NAME' env var needed with 'DEV_FLAG'"
        ):
            importlib.reload(configuration)
