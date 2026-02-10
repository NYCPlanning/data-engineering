from typing import Literal

import pytest

from dcpy.utils import introspect


@pytest.mark.parametrize(
    "obj, cls, notcls",
    [
        ("a", Literal["a"], Literal["b"]),
        ("a", Literal["a"] | int, Literal["b"] | int),
        (["a"], list[str], list[int]),
        (["a", 1], list[str | int], list[int | Literal["b"]]),
        ({"a": 1}, dict[str, int], dict[int, int]),
    ],
)
def test_isinstance(obj, cls, notcls):
    assert introspect._isinstance(obj, cls)
    assert not introspect._isinstance(obj, notcls)


def _helper(output: dict[str, str], expected: list[str], assertion_error_message: str):
    assert set(expected) == set(output.keys()), assertion_error_message


class TestValidateKwargs:
    """
    Test the validate_kwargs function.

    Ensures that a variety of cases - missing args, unexpected args, mistyped args, etc are caught
    Tests are a bit redundant, code-wise, for clarity in debugging if errors are found
    """

    # Variety of dummy data. All roughly follows same signature
    a = {"a": "a"}
    ab = {"a": "a", "b": 1}
    ab_wrong_type = {"a": 1, "b": "b", "c": True}
    abc = {"a": "a", "b": 1, "c": True}
    abcd = {"a": "a", "b": 1, "c": True, "d": "goodbye"}

    # Function with no arguments
    @pytest.mark.parametrize(
        "input, expected_output, error_message",
        [
            ({}, [], "No arguments supplied to noargs"),
            (a, ["a"], "Unexpected argument to noargs not caught"),
        ],
    )
    def test_noargs(self, input, expected_output, error_message):
        def noargs():
            return

        _helper(
            introspect.validate_kwargs(noargs, input),
            expected_output,
            error_message,
        )

    # Function with positional arguments
    @pytest.mark.parametrize(
        "input, expected_output, error_message",
        [
            (ab, [], "Both arguments supplied to args"),
            (a, ["b"], "Missing argument to args not caught"),
            (
                ab_wrong_type,
                ["a", "b", "c"],
                "Type mismatch and unexpected argument to args not caught",
            ),
        ],
    )
    def test_args(self, input, expected_output, error_message):
        def args(a: str, b: int):
            return

        _helper(
            introspect.validate_kwargs(args, input),
            expected_output,
            error_message,
        )

    # Function with positional arguments and one default value
    @pytest.mark.parametrize(
        "input, expected_output, error_message",
        [
            (a, [], "Valid missing arg with default to args_default"),
            (ab, [], "Both args supplied to args_default"),
        ],
    )
    def test_args_default(self, input, expected_output, error_message):
        def args_default(a: str, b: int = 0):
            return

        _helper(
            introspect.validate_kwargs(args_default, input),
            expected_output,
            error_message,
        )

    # Function with **kwargs
    @pytest.mark.parametrize(
        "input, expected_output, error_message",
        [
            (abc, [], "Valid extra kwarg supplied to star_kwargs"),
        ],
    )
    def test_star_kwargs(self, input, expected_output, error_message):
        def star_kwargs(a: str, b: int, **kwargs):
            return

        _helper(
            introspect.validate_kwargs(star_kwargs, input),
            expected_output,
            error_message,
        )

    # Function with kwargs
    @pytest.mark.parametrize(
        "input, expected_output, error_message",
        [
            (a, ["b", "c"], "missing positional and kw args b and c"),
            (abc, [], "all arguments supplied"),
        ],
    )
    def test_kwargs(self, input, expected_output, error_message):
        def kwargs(a: str, b: int, *, c: bool):
            return

        _helper(
            introspect.validate_kwargs(kwargs, input),
            expected_output,
            error_message,
        )

    # Function with *args. Not supported
    def test_star_args(self):
        def star_args(a: str, *args):
            return

        with pytest.raises(TypeError, match="Positional args not supported"):
            introspect.validate_kwargs(star_args, self.abc)

    # Function with everything
    # For this, test both presence and absence of variables in returned dict
    @pytest.mark.parametrize(
        "input, expected_output, error_message",
        [
            ({}, ["a", "c"], "missing arguments a and c, while b and d have defaults"),
            (a, ["c"], "missing argument c. a supplied, b and d have defaults"),
            (
                ab_wrong_type,
                ["a", "b"],
                "a and b have incorrect types. c supplied, d has default",
            ),
            (abc, [], "a, b, c supplied. d has default"),
            (abcd, [], "all arguments supplied"),
        ],
    )
    def test_all(self, input, expected_output, error_message):
        def kwargs_default(a: str, b: int = 1, *, c: bool, d: str = "Hello!"):
            return

        empty = introspect.validate_kwargs(kwargs_default, {})
        assert (
            "a" in empty
            and ("b" not in empty)
            and ("c" in empty)
            and ("d" not in empty)
        )
        only_a = introspect.validate_kwargs(kwargs_default, self.a)
        assert (
            ("a" not in only_a)
            and ("b" not in only_a)
            and ("c" in only_a)
            and ("d" not in only_a)
        ), "Supplying only 'a' to kwargs_default should error"
        type_e = introspect.validate_kwargs(kwargs_default, self.ab_wrong_type)
        assert (
            ("a" in type_e)
            and ("b" in type_e)
            and ("c" not in type_e)
            and ("d" not in type_e)
        ), "Supplying wrong types to kwargs_default should error"
        assert not introspect.validate_kwargs(kwargs_default, self.abc), (
            "Supplying abc to kwargs_default should not error"
        )
        assert not introspect.validate_kwargs(kwargs_default, self.abcd), (
            "Supplying abcd to kwargs_default should not error"
        )
