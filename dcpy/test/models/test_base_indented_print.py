import pandas as pd

from dcpy.models.base import IndentedPrint


class Inner(IndentedPrint):
    _pretty_print_fields = False
    a: str
    b: list[str]
    c: dict[str, str]
    d: pd.DataFrame


class Outer(IndentedPrint):
    _display_names = {"a": "First field 'a'"}
    _title = "Outer Indented Print"
    _include_line_breaks = True
    a: set[str]
    b: list[str]
    c: dict[str, str]
    d: Inner


df = pd.DataFrame({"x": [1, 2], "y": [3, 4]})

inner = Inner(a="a", b=["b", "b"], c={"c": "c", "e": "f"}, d=df)

outer = Outer(a={"a", "b"}, b=["bb", "bbb"], c={"cc": "ccc"}, d=inner)


def test():
    assert outer.to_list_structure() == [
        "Outer Indented Print",
        "________________________________________________________________________________",
        "First field 'a':",
        {"a", "b"},
        "________________________________________________________________________________",
        "B:",
        ["bb", "bbb"],
        "________________________________________________________________________________",
        "C:",
        {"cc": "ccc"},
        "________________________________________________________________________________",
        "D:",
        ["a: a", "b:", ["b", "b"], "c:", {"c": "c", "e": "f"}, "d", df],
    ]

    assert outer.indent_and_flatten(
        outer.to_list_structure(), indent="    ", offset=""
    ) == [
        "Outer Indented Print",
        "________________________________________________________________________________",
        "First field 'a':",
        "    a",
        "    b",
        "________________________________________________________________________________",
        "B:",
        "    bb",
        "    bbb",
        "________________________________________________________________________________",
        "C:",
        "    cc",
        "        ccc",
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
        "           x  y",
        "        0  1  3",
        "        1  2  4",
    ]
