from enum import Enum
import inspect
from types import UnionType
from typing import Callable, get_args, get_origin, Literal, Union


def _isempty(obj) -> bool:
    return obj == inspect._empty


def _isinstance(obj, cls, strict_enums=True) -> bool:
    """
    _isinstance extends behavior of basic python `isinstance` to allow for
    parameterized generics (i.e. check if an object is dict[str, str])
    """
    if get_origin(cls) is Literal:
        return obj in get_args(cls)
    elif get_origin(cls) in (Union, UnionType):
        return any([_isinstance(obj, param) for param in get_args(cls)])
    elif get_origin(cls) is list:
        param = get_args(cls)[0]
        return isinstance(obj, list) and all([_isinstance(e, param) for e in obj])
    elif get_origin(cls) is dict:
        keytype = get_args(cls)[0]
        valtype = get_args(cls)[1]
        return (
            isinstance(obj, dict)
            and all([_isinstance(key, keytype) for key in obj])
            and all([_isinstance(obj[key], valtype) for key in obj])
        )
    elif issubclass(cls, Enum):
        return isinstance(obj, cls) or (not strict_enums and obj in cls)
    else:
        return isinstance(obj, cls)


def validate_kwargs(
    function: Callable,
    kwargs: dict,
    raise_error=False,
    ignore_args: list[str] | None = None,
    strict_enums: bool = True,
) -> dict[str, str]:
    """
    Given a function and dict containing kwargs, validates that kwargs satiffy the
    signature of the function. Violations are returned as a dict, with argument names
    as keys and types of violation as value. Types of violation can be either a missing argument,
    an argument of the wrong type, or an unexpected argument.

    While this can handle functions with "positional or keyword" args, cannot handle positional only arguments
    (function type signatures with slashes or with *args)

    If raise_error flag supplied, raises error instead of returning dict of violations.
    """
    ignore_args = ignore_args or []
    sig = inspect.signature(function)

    pos = [
        a
        for a in sig.parameters.values()
        if a.kind == a.VAR_POSITIONAL or a.kind == a.POSITIONAL_ONLY
    ]
    if pos:
        raise TypeError(
            f"Positional args not supported in `validate_kwargs`, {function.__name__} is invalid"
        )

    expected_args = [
        a
        for a in sig.parameters
        if a not in ignore_args
        and sig.parameters[a].kind != sig.parameters[a].VAR_KEYWORD
    ]
    defaults = [a for a in expected_args if not _isempty(sig.parameters[a].default)]
    violating_args = {}

    for arg_name in expected_args:
        if arg_name in kwargs:
            arg = kwargs[arg_name]
            annotation = sig.parameters[arg_name].annotation
            if not (
                _isempty(annotation)
                or isinstance(annotation, str)
                or _isinstance(arg, annotation, strict_enums=strict_enums)
            ):
                violating_args[arg_name] = (
                    f"Type mismatch, expected '{annotation}' and got {type(arg)}"
                )
        elif arg_name not in defaults:
            violating_args[arg_name] = "Missing"

    ## if **kwargs is present
    varkw = [a for a in sig.parameters.values() if a.kind == a.VAR_KEYWORD]

    if not varkw:
        for arg in kwargs:
            if arg not in expected_args and arg not in ignore_args:
                violating_args[arg] = "Unexpected"

    if violating_args and raise_error:
        raise TypeError(
            f"Function spec mismatch for function {function.__name__}. Violating arguments:\n{violating_args}"
        )

    return violating_args
