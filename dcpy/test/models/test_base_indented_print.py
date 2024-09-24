import pandas as pd

from dcpy.models.base import IndentedPrint


class Inner(IndentedPrint):
    _pretty_print_fields = False
    _max_df_length = 1
    a: str
    b: list
    c: dict[str, str]
    d: pd.DataFrame


class Outer(IndentedPrint):
    _display_names = {"a": "First field 'a'"}
    _title = "Outer Indented Print"
    _include_line_breaks = True
    a: set[str]
    b: pd.DataFrame
    c: dict[str, str]
    d: Inner


df = pd.DataFrame({"x": [1, 2], "y": [3, 4]})

inner = Inner(a="a", b=["b", "b"], c={"c": "c", "e": "f"}, d=df)

outer = Outer(a={"a", "b"}, b=df, c={}, d=inner)


def test():
    assert outer.to_list_structure() == [
        "Outer Indented Print",
        "________________________________________________________________________________",
        "First field 'a':",
        {"b", "a"},
        "________________________________________________________________________________",
        "B:",
        df,
        "________________________________________________________________________________",
        "C: None",
        "________________________________________________________________________________",
        "D:",
        inner,
    ]

    assert outer.indent_and_flatten(indent="    ", offset="") == [
        "Outer Indented Print",
        "________________________________________________________________________________",
        "First field 'a':",
        "    a",
        "    b",
        "________________________________________________________________________________",
        "B:",
        "       x  y",
        "    0  1  3",
        "    1  2  4",
        "________________________________________________________________________________",
        "C: None",
        "________________________________________________________________________________",
        "D:",
        "    a: a",
        "    b:",
        "        b",
        "        b",
        "    c:",
        "        c",
        "            c",
        "        e",
        "            f",
        "    d:",
        "    2 rows. First 1 shown",
        "           x  y",
        "        0  1  3",
    ]
