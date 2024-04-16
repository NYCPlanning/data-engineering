from datetime import datetime
from pathlib import Path
import pytest
import yaml

from dcpy.models import file
from dcpy.models.lifecycle.ingest import Template
from dcpy.models.connectors import web
from dcpy.lifecycle.ingest import TEMPLATE_DIR, configure

RESOURCES = Path(__file__).parent / "resources"


def test_validate_all_datasets():
    templates = [t for t in TEMPLATE_DIR.glob("*")]
    assert len(templates) > 0
    for file in templates:
        with open(file, "r") as f:
            s = yaml.safe_load(f)
        val = Template(**s)
        assert val


def test_jinja_vars():
    no_vars = configure.get_jinja_vars("fake_yml: value")
    assert len(no_vars) == 0, "No variables should have been found"
    vars = configure.get_jinja_vars(r"fake_yml: {{ version }}")
    assert vars == {"version"}, "One var, 'version', should have been found"


def test_read_template():
    with pytest.raises(
        Exception,
        match="Version must be supplied explicitly to be rendered in template",
    ):
        configure.read_template("dcp_atomicpolygons")

    with pytest.raises(
        Exception,
        match="'version' is only suppored jinja var. Unsupported vars in template: ",
    ):
        configure.read_template(
            "invalid_jinja", version="dummy", template_dir=RESOURCES
        )

    template = configure.read_template("dcp_atomicpolygons", version="test")
    assert isinstance(template.source, web.FileDownloadSource)
    assert (
        template.source.url
        == "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/nyap_test.zip"
    )
    assert isinstance(
        template.file_format,
        file.Shapefile,
    )


def test_get_config():
    template = configure.read_template("dcp_atomicpolygons", version="test")
    config = configure.get_config(
        template, version="test", timestamp=datetime.now(), file_name="dummy.txt"
    )
    assert True  # really just need to make sure that templates properly get converted into configs


def test_validate_function_args():
    """
    Test the validate_function_args function.

    Ensures that a variety of cases - missing args, unexpected args, mistyped args, etc are caught
    """
    # Variety of dummy data. All roughly follows same signature
    a = {"a": "a"}
    ab = {"a": "a", "b": 1}
    ab_wrong_type = {"a": 1, "b": "b", "c": True}
    abc = {"a": "a", "b": 1, "c": True}
    abcd = {"a": "a", "b": 1, "c": True, "d": "goodbye"}

    # Function with no arguments
    def noargs():
        return

    # No arguments passed
    assert not configure.validate_function_args(
        noargs, {}
    ), "No arguments supplied to noargs"
    # Unexpected argument returns value (error message not validated, just presence of it in returned dict)
    assert "a" in configure.validate_function_args(
        noargs, a
    ), "Unexpected argument to noargs not caught"

    # Function with positional arguments
    def args(a: str, b: int):
        return

    # Two valid arguments
    assert not configure.validate_function_args(
        args, ab
    ), "Both arguments supplied to args"
    # Missing argument is returned
    assert "b" in configure.validate_function_args(
        args, a
    ), "Missing argument to args not caught"
    # Type mismatch of arguments and additonal argument. All 3 errors should be present
    multiple_errors = configure.validate_function_args(args, ab_wrong_type)
    assert (
        "a" in multiple_errors and "b" in multiple_errors and "c" in multiple_errors
    ), "Type mismatch and unexpected argument to args not caught"

    # Function with positional arguments and one default value
    def args_default(a: str, b: int = 0):
        return

    # Default not supplied
    assert not configure.validate_function_args(
        args_default, a
    ), "Valid missing arg with default to args_default"
    # Default supplied
    assert not configure.validate_function_args(
        args_default, ab
    ), "Both args supplied to args_default"

    # Function with **kwargs
    def star_kwargs(a: str, b: int, **kwargs):
        return

    # No error with "unexpected" argument
    assert not configure.validate_function_args(
        star_kwargs, abc
    ), "Valid extra kwarg supplied to star_kwargs"

    # Function with kwargs
    def kwargs(a: str, b: int, *, c: bool):
        return

    multiple_errors = configure.validate_function_args(kwargs, a)
    assert (
        "b" in multiple_errors and "c" in multiple_errors
    ), "Missing positional and kwargs in kwargs not caught"
    assert not configure.validate_function_args(
        kwargs, abc
    ), "All arguments supplied to kwargs"

    # Function with *args. Not supported
    def star_args(a: str, *args):
        return

    with pytest.raises(TypeError, match="Positional args not supported"):
        configure.validate_function_args(star_args, abc)

    # Function with everything
    # For this, test both presence and absence of variables in returned dict
    def kwargs_default(a: str, b: int = 1, *, c: bool, d: str = "Hello!"):
        return

    empty = configure.validate_function_args(kwargs_default, {})
    assert "a" in empty and ("b" not in empty) and ("c" in empty) and (not "d" in empty)
    only_a = configure.validate_function_args(kwargs_default, a)
    assert (
        ("a" not in only_a)
        and ("b" not in only_a)
        and ("c" in only_a)
        and ("d" not in only_a)
    ), "Supplying only 'a' to kwargs_default should error"
    type_e = configure.validate_function_args(kwargs_default, ab_wrong_type)
    assert (
        ("a" in type_e)
        and ("b" in type_e)
        and ("c" not in type_e)
        and ("d" not in type_e)
    ), "Supplying wrong types to kwargs_default should error"
    assert not configure.validate_function_args(
        kwargs_default, abc
    ), "Supplying abc to kwargs_default should not error"
    assert not configure.validate_function_args(
        kwargs_default, abcd
    ), "Supplying abcd to kwargs_default should not error"
