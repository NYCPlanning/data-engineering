from __future__ import annotations
import inspect
from datetime import datetime
import jinja2
from jinja2 import meta
from pathlib import Path
from typing import Callable
import yaml

from dcpy.models.lifecycle.ingest import Template, Config

from dcpy.utils.logging import logger
from dcpy.models.connectors import socrata, edm
from dcpy.connectors.socrata import extract as extract_socrata
from dcpy.connectors.edm import publishing
from . import TEMPLATE_DIR


def get_jinja_vars(s: str) -> set[str]:
    """Get all variables expected in a jinja template string"""
    env = jinja2.Environment()
    parsed_content = env.parse(s)
    return meta.find_undeclared_variables(parsed_content)


def read_template(
    dataset: str, version: str | None = None, template_dir: Path = TEMPLATE_DIR
) -> Template:
    """Given dataset name, read yml template in template_dir of given dataset
    and insert version as jinja var if provided."""
    file = template_dir / f"{dataset}.yml"
    logger.info(f"Reading template from {file}")
    with open(file, "r") as f:
        template_string = f.read()
    vars = get_jinja_vars(template_string)
    if not version and len(vars) > 0:
        if vars == {"version"}:
            raise Exception(
                "Version must be supplied explicitly to be rendered in template"
            )
        else:
            raise Exception(f"Unsupported jinja vars found in template: {vars}")
    else:
        if len(vars) > 0 and vars != {"version"}:
            vars.discard("version")
            raise Exception(
                f"'version' is only suppored jinja var. Unsupported vars in template: {vars}"
            )
        template_string = jinja2.Template(template_string).render(version=version)
    template_yml = yaml.safe_load(template_string)
    return Template(**template_yml)


def get_version(template: Template, timestamp: datetime) -> str:
    """Given parsed dataset template, determine version.
    If version's source has no custom logic, returns formatted date
    from provided datetime"""
    match template.source:
        case socrata.Source() as socrata_source:
            return extract_socrata.get_version(socrata_source)
        case edm.publishing.GisDataset() as gis_dataset:
            return publishing.get_latest_gis_dataset_version(gis_dataset.name)
        case _:
            return timestamp.strftime("%Y%m%d")


def get_config(
    template: Template, version: str, timestamp: datetime, file_name: str
) -> Config:
    """Simple wrapper to produce a recipes ExtractConfig from a parsed template
    and other computed values"""
    return Config(
        version=version,
        archival_timestamp=timestamp,
        raw_filename=file_name,
        **template.model_dump(),
    )


# This maybe belongs in a more pure util section of code
def validate_function_args(
    function: Callable, kwargs: dict, raise_error=False
) -> dict[str, str]:
    """
    Given a function and dict containing kwargs, validates that kwargs satiffy the
    signature of the function. Violations are returned as a dict, with argument names
    as keys and types of violation as value. Types of violation can be either a missing argument,
    an argument of the wrong type, or an unexpected argument.
    If raise_error flag supplied, raises error instead of returning dict of violations.
    """
    spec = inspect.getfullargspec(function)
    if spec.varargs is not None:
        raise TypeError(
            f"Positional args not supported, {function.__name__} is invalid preprocessing function"
        )

    defaults = {}
    if spec.defaults:
        defaults_start = len(spec.args) - len(spec.defaults)
        for i, default in enumerate(spec.defaults):
            defaults[spec.args[defaults_start + i]] = default

    if spec.kwonlydefaults:
        defaults.update(spec.kwonlydefaults)

    expected_args = spec.args + spec.kwonlyargs
    violating_args = {}

    for arg_name in expected_args:
        if arg_name in kwargs:
            if arg_name in spec.annotations:
                arg = kwargs[arg_name]
                annotation = spec.annotations[arg_name]
                if not isinstance(arg, annotation):
                    violating_args[
                        arg_name
                    ] = f"Type mismatch, expected '{annotation}' and got {type(arg)}"
        elif arg_name not in defaults:
            violating_args[arg_name] = "Missing"

    if spec.varkw is None:
        for arg in kwargs:
            if arg not in expected_args:
                violating_args[arg] = "Unexpected"

    if violating_args and raise_error:
        raise TypeError(
            f"Function spec mismatch for function {function.__name__}. Violating arguments:\n{violating_args}"
        )

    return violating_args
