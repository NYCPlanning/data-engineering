import jinja2
from pandas import DataFrame
from pathlib import Path
from pydantic import BaseModel, model_serializer
from pydantic.fields import PrivateAttr
import typing
import yaml


from dcpy.utils.logging import logger


class SortedSerializedBase(BaseModel):
    """A Pydantic BaseModel that will allow for sensible (and overrideable) deserialization order.

    The serialization order is as follows:
    - model attributes defined in the head sort order
    - simple (and nullable simple) types: strings, ints, bools, literals
    - complex types
    - model attributes defined in the tail sort order

    Note: This is put in its own class because it might be useful for other
    classes unrelated to product metadata in the future.
    """

    _exclude_falsey_values: bool = True
    _head_sort_order: list[str] = PrivateAttr(default=["id"])
    _tail_sort_order: list[str] = PrivateAttr(default=["custom"])

    @model_serializer(mode="wrap")
    def _model_dump_ordered(self, handler):
        unordered = handler(self)

        ordered_items_head = []
        ordered_items_tail = []
        simple_type_items = []
        other_items = []

        for model_field in list(unordered.items()):
            model_key, model_val = model_field

            if not model_val and model_val != 0 and self._exclude_falsey_values:
                # If an object's values are all None, it will serialize as {}.
                # These aren't removed by model_dump(exclude_none=True), so we have to do it manually.
                continue
            field_type = self.model_fields[
                model_key
            ].annotation  # Need to retrieve type from the class def, not the instance
            is_literal = type(field_type) is typing._LiteralGenericAlias  # type: ignore
            simple_types = {
                bool,
                bool | None,  # This is a little hacky, but does the job.
                str,
                str | None,
                int,
                int | None,
                float,
                float | None,
                type(None),
            }

            if model_key in self._head_sort_order:
                ordered_items_head.append(model_field)
            elif model_key in self._tail_sort_order:
                ordered_items_tail.append(model_field)
            elif field_type in simple_types or is_literal:
                simple_type_items.append(model_field)
            else:
                other_items.append(model_field)

        # As of python 3.7, dict keys are ordered, and so will serialize in the order below
        return dict(
            sorted(ordered_items_head, key=lambda x: self._head_sort_order.index(x[0]))
            + sorted(simple_type_items, key=lambda x: x[0])
            + sorted(other_items, key=lambda x: x[0])
            + sorted(
                ordered_items_tail, key=lambda x: self._tail_sort_order.index(x[0])
            )
        )


class YamlWriter(BaseModel):
    class _YamlTopLevelSpacesDumper(yaml.SafeDumper):
        """YAML serializer that will insert lines between top-level entries,
        which is nice in longer files."""

        def write_line_break(self, data=None):
            super().write_line_break(data)

            if len(self.indents) == 1:
                super().write_line_break()

    def write_to_yaml(self, path: Path):
        def str_presenter(dumper, data):
            # To maintain readabily for dumping multiline strings. Otherwise the dumped text has no consistency,
            # leading to, sometimes:
            # \ text of varying\n\n \
            # \ readability\n\n \
            if len(data.splitlines()) > 1:  # check for multiline string
                return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
            return dumper.represent_scalar("tag:yaml.org,2002:str", data)

        yaml.add_representer(str, str_presenter)
        yaml.representer.SafeRepresenter.add_representer(str, str_presenter)

        with open(path, "w", encoding="utf8") as f:
            f.write(
                yaml.dump(
                    self.model_dump(exclude_none=True),
                    sort_keys=False,
                    default_flow_style=False,
                    Dumper=YamlWriter._YamlTopLevelSpacesDumper,
                    allow_unicode=True,
                )
            )


class TemplatedYamlReader(BaseModel):
    @classmethod
    def from_yaml(cls, yaml_str: str, *, template_vars=None) -> typing.Self:
        if template_vars:
            logger.debug(f"Templating metadata with vars: {template_vars}")
            templated = jinja2.Template(
                yaml_str, undefined=jinja2.StrictUndefined
            ).render(template_vars or {})
            return cls(**yaml.safe_load(templated))
        else:
            logger.debug("No Template vars supplied. Skipping templating.")
        return cls(**yaml.safe_load(yaml_str))

    @classmethod
    def from_path(cls, path: Path, *, template_vars=None) -> typing.Self:
        with open(path, "r", encoding="utf-8") as raw:
            return cls.from_yaml(raw.read(), template_vars=template_vars)


class IndentedPrint(BaseModel, arbitrary_types_allowed=True):
    """
    A class meant to print out a relatively nicely formatted layout of its nested structure
    It has a few main attribute functions.

    The IntendedPrint represented by
    {
        "a": "b",
        "c": ["d", "e"],
        "f": {
            "g": ["h"],
        },
    }
    Would output the following from `to_list_structure`
    [
        "a: b",
        "c",
        ["d", "e"],
        "f",
        ["g", ["h"]],
    ]
    Which then fed to `indent_and_flatten` would output
    [
        "a",
        "b",
        "c",
        "    d",
        "    e",
        "f",
        "    g",
        "        h",
    ]
    Which would then be printed line by line by `report`
    """

    _title: str | None = PrivateAttr(default=None)
    _include_line_breaks: bool = PrivateAttr(default=False)
    _pretty_print_fields: bool = PrivateAttr(default=True)
    _display_names: dict[str, str] = PrivateAttr(default={})
    _max_df_length: int = PrivateAttr(default=20)

    def to_list_structure(self):
        a = []
        if self._title:
            a.append(self._title)
        for field in self.model_fields:
            value = getattr(self, field)

            if field in self._display_names:
                field = self._display_names[field]
            elif self._pretty_print_fields:
                field = field.replace("_", " ").capitalize()
            if self._include_line_breaks:
                a.append("_" * 80)

            if not isinstance(value, DataFrame) and not value and value != 0:
                value_element = None
            else:
                value_element = value

            if (
                isinstance(value_element, str)
                or isinstance(value_element, int)
                or isinstance(value_element, float)
                or isinstance(value_element, bool)
                or value_element is None
            ):
                a.append(f"{field}: {value_element}")
            else:
                a += [field + ":", value_element]
        return a

    def indent_and_flatten(self, indent: str, offset: str):
        def apply_indent(el, level):
            i = offset + indent * level
            return i + str(el)

        def flatten(li: list, level) -> list:
            if li == []:
                return li
            if isinstance(li[0], IndentedPrint):
                first = li[0].indent_and_flatten(
                    indent=indent, offset=offset + indent * (level + 1)
                )
            elif isinstance(li[0], list):
                first = flatten(li[0], level + 1)
            elif isinstance(li[0], set):
                first = flatten(sorted(list(li[0])), level + 1)
            elif isinstance(li[0], dict):
                first = []
                for key in li[0]:
                    first.append(key)
                    first.append([li[0][key]])
                first = flatten(first, level + 1)
            elif isinstance(li[0], DataFrame):
                df = li[0]
                if len(df) > self._max_df_length:
                    first = [
                        apply_indent(
                            f"{len(df)} rows. First {self._max_df_length} shown", level
                        )
                    ] + flatten(
                        df.head(self._max_df_length).to_string().split("\n"), level + 1
                    )
                else:
                    first = flatten(df.to_string().split("\n"), level + 1)
            else:
                first = [apply_indent(li[0], level)]
            return first + flatten(li[1:], level)

        return flatten(self.to_list_structure(), level=0)

    def report(self):
        return "\n".join(self.indent_and_flatten(offset="", indent="    "))
