import pandas as pd
from pydantic import BaseModel

from dcpy.utils import collections


def test_deep_merge():
    assert collections.deep_merge_dict(
        {"a": "1", "b": {"c": {"d": "e"}}},
        {"a": "1", "b": {"c": {"x": "y"}}},
    ) == {"a": "1", "b": {"c": {"d": "e", "x": "y"}}}


class Inner(BaseModel, arbitrary_types_allowed=True):
    g: str
    i: list
    m: dict[str, str]
    r: pd.DataFrame


class Outer(BaseModel, arbitrary_types_allowed=True):
    a: set[str]
    d: pd.DataFrame
    e: dict[str, str]
    f: Inner


class TestIndentedPrint:
    df = pd.DataFrame({"x": [1, 2], "y": [3, 4]})
    inner = Inner(g="h", i=[["j"], {"k", "l"}], m={"n": "o", "p": "q"}, r=df)
    outer = Outer(a={"b", "c"}, d=df, e={}, f=inner)

    def test(self):
        assert (
            collections.flatten_and_indent(self.outer.model_dump())
            == [
                "a",
                "    b",
                "    c",
                "d",
                "       x  y",
                "    0  1  3",
                "    1  2  4",
                "e: None",
                "f",
                "    g: h",
                "    i",
                "            j",  # list of lists are a little awkward. Not really a common use case though
                "            k",
                "            l",
                "    m",
                "        n: o",
                "        p: q",
                "    r",
                "           x  y",
                "        0  1  3",
                "        1  2  4",
            ]
        )

    def test_custom_indent_and_offset(self):
        assert collections.flatten_and_indent(
            self.outer.model_dump(), indent="  ", offset=" "
        ) == [
            " a",
            "   b",
            "   c",
            " d",
            "      x  y",
            "   0  1  3",
            "   1  2  4",
            " e: None",
            " f",
            "   g: h",
            "   i",
            "       j",
            "       k",
            "       l",
            "   m",
            "     n: o",
            "     p: q",
            "   r",
            "        x  y",
            "     0  1  3",
            "     1  2  4",
        ]

    def test_line_breaks(self):
        assert collections.flatten_and_indent(
            self.outer.model_dump(), include_line_breaks=True
        ) == [
            "________________________________________________________________________________",
            "a",
            "    b",
            "    c",
            "________________________________________________________________________________",
            "d",
            "       x  y",
            "    0  1  3",
            "    1  2  4",
            "________________________________________________________________________________",
            "e: None",
            "________________________________________________________________________________",
            "f",
            "    g: h",
            "    i",
            "            j",
            "            k",
            "            l",
            "    m",
            "        n: o",
            "        p: q",
            "    r",
            "           x  y",
            "        0  1  3",
            "        1  2  4",
        ]

    def test_pretty_print(self):
        assert collections.flatten_and_indent(
            self.outer.model_dump(), pretty_print_fields=True
        ) == [
            "A",
            "    b",
            "    c",
            "D",
            "       x  y",
            "    0  1  3",
            "    1  2  4",
            "E: None",
            "F",
            "    G: h",
            "    I",
            "            j",
            "            k",
            "            l",
            "    M",
            "        N: o",
            "        P: q",
            "    R",
            "           x  y",
            "        0  1  3",
            "        1  2  4",
        ]

    def test_to_report(self):
        collections.indented_report(self.outer.model_dump()) == """a
            b
            c
        d
               x  y
            0  1  3
            1  2  4
        e: None
        f
            g: h
            i
                    j
                    k
                    l
            m
                n: o
                p: q
            r
                   x  y
                0  1  3
                1  2  4"""
