import inspect
from types import UnionType
from typing import Callable, get_args, get_origin, Literal


def _isinstance(obj, cls):
    """
    _isinstance extends behavior of basic python `isinstance` to allow for
    parameterized generics (i.e. check if an object is dict[str, str])
    """
    if get_origin(cls) == Literal:
        return obj in get_args(cls)
    elif get_origin(cls) == UnionType:
        return any([_isinstance(obj, param) for param in get_args(cls)])
    elif get_origin(cls) == list:
        param = get_args(cls)[0]
        return isinstance(obj, list) and all([_isinstance(l, param) for l in obj])
    elif get_origin(cls) == dict:
        keytype = get_args(cls)[0]
        valtype = get_args(cls)[1]
        return (
            isinstance(obj, dict)
            and all([_isinstance(key, keytype) for key in obj])
            and all([_isinstance(obj[key], valtype) for key in obj])
        )
    else:
        return isinstance(obj, cls)


# This maybe belongs in a more pure util section of code
def validate_function_args(
    function: Callable,
    kwargs: dict,
    raise_error=False,
    ignore_args: list[str] | None = None,
) -> dict[str, str]:
    """
    Given a function and dict containing kwargs, validates that kwargs satiffy the
    signature of the function. Violations are returned as a dict, with argument names
    as keys and types of violation as value. Types of violation can be either a missing argument,
    an argument of the wrong type, or an unexpected argument.
    If raise_error flag supplied, raises error instead of returning dict of violations.
    """
    ignore_args = ignore_args or []
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
    expected_args = [a for a in expected_args if a not in ignore_args]
    violating_args = {}

    for arg_name in expected_args:
        if arg_name in kwargs:
            if arg_name in spec.annotations:
                arg = kwargs[arg_name]
                annotation = spec.annotations[arg_name]
                if not _isinstance(arg, annotation):
                    violating_args[arg_name] = (
                        f"Type mismatch, expected '{annotation}' and got {type(arg)}"
                    )
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
