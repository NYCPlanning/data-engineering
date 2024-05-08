import pytest
from dcpy.utils import introspect


class TestValidateFunctionArgs:
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
    def test_noargs(self):
        def noargs():
            return

        # No arguments passed
        assert not introspect.validate_function_args(
            noargs, {}
        ), "No arguments supplied to noargs"
        # Unexpected argument returns value (error message not validated, just presence of it in returned dict)
        assert "a" in introspect.validate_function_args(
            noargs, self.a
        ), "Unexpected argument to noargs not caught"

    # Function with positional arguments
    def test_args(self):
        def args(a: str, b: int):
            return

        # Two valid arguments
        assert not introspect.validate_function_args(
            args, self.ab
        ), "Both arguments supplied to args"
        # Missing argument is returned
        assert "b" in introspect.validate_function_args(
            args, self.a
        ), "Missing argument to args not caught"
        # Type mismatch of arguments and additonal argument. All 3 errors should be present
        multiple_errors = introspect.validate_function_args(args, self.ab_wrong_type)
        assert (
            "a" in multiple_errors and "b" in multiple_errors and "c" in multiple_errors
        ), "Type mismatch and unexpected argument to args not caught"

    # Function with positional arguments and one default value
    def test_args_default(self):
        def args_default(a: str, b: int = 0):
            return

        # Default not supplied
        assert not introspect.validate_function_args(
            args_default, self.a
        ), "Valid missing arg with default to args_default"
        # Default supplied
        assert not introspect.validate_function_args(
            args_default, self.ab
        ), "Both args supplied to args_default"

    # Function with **kwargs
    def test_star_kwargs(self):
        def star_kwargs(a: str, b: int, **kwargs):
            return

        # No error with "unexpected" argument
        assert not introspect.validate_function_args(
            star_kwargs, self.abc
        ), "Valid extra kwarg supplied to star_kwargs"

    # Function with kwargs
    def test_kwargs(self):
        def kwargs(a: str, b: int, *, c: bool):
            return

        multiple_errors = introspect.validate_function_args(kwargs, self.a)
        assert (
            "b" in multiple_errors and "c" in multiple_errors
        ), "Missing positional and kwargs in kwargs not caught"
        assert not introspect.validate_function_args(
            kwargs, self.abc
        ), "All arguments supplied to kwargs"

    # Function with *args. Not supported
    def test_star_args(self):
        def star_args(a: str, *args):
            return

        with pytest.raises(TypeError, match="Positional args not supported"):
            introspect.validate_function_args(star_args, self.abc)

    # Function with everything
    # For this, test both presence and absence of variables in returned dict
    def test_all(self):
        def kwargs_default(a: str, b: int = 1, *, c: bool, d: str = "Hello!"):
            return

        empty = introspect.validate_function_args(kwargs_default, {})
        assert (
            "a" in empty
            and ("b" not in empty)
            and ("c" in empty)
            and (not "d" in empty)
        )
        only_a = introspect.validate_function_args(kwargs_default, self.a)
        assert (
            ("a" not in only_a)
            and ("b" not in only_a)
            and ("c" in only_a)
            and ("d" not in only_a)
        ), "Supplying only 'a' to kwargs_default should error"
        type_e = introspect.validate_function_args(kwargs_default, self.ab_wrong_type)
        assert (
            ("a" in type_e)
            and ("b" in type_e)
            and ("c" not in type_e)
            and ("d" not in type_e)
        ), "Supplying wrong types to kwargs_default should error"
        assert not introspect.validate_function_args(
            kwargs_default, self.abc
        ), "Supplying abc to kwargs_default should not error"
        assert not introspect.validate_function_args(
            kwargs_default, self.abcd
        ), "Supplying abcd to kwargs_default should not error"
