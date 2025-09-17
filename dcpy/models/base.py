import jinja2
import json
import pandas as pd
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
    _repr_functions: dict[str, typing.Callable[[typing.Any], str]] = {}

    def field_repr(self, field_name: str):
        """overrideable method to mimic __repr__ when we have class attributes that
        aren't pydantic classes. e.g. list[DatasetColumn]"""
        attr = getattr(self, field_name)
        if field_name in self._repr_functions:
            repr_fn = self._repr_functions[field_name]
            return repr_fn(attr)
        return str(attr or "")

    def all_fields_repr(self) -> dict[str, str]:
        return {k: self.field_repr(k) for k, _ in self.model_fields.items()}

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

    def dump_json(
        self,
        filepath: Path,
        *,
        exclude_none: bool = True,
        exclude_defaults: bool = True,
        indent: int = 4,
        **kwargs,
    ):
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(
                self.model_dump(
                    mode="json",
                    exclude_none=exclude_none,
                    exclude_defaults=exclude_defaults,
                    **kwargs,
                ),
                f,
                indent=indent,
            )


class YamlWriter(BaseModel):
    _exclude_none: bool = True

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
                    self.model_dump(exclude_none=self._exclude_none),
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


class ModelWithDataFrame(BaseModel, arbitrary_types_allowed=True):
    def __eq__(self, other):
        """
        pandas DataFrames make simple equality checks a pain
        naively checking equality of df-containing model produces 'ValueError: The truth value of a DataFrame is ambiguous'
        therefore, df.equals() must be used

        Only supported types so far are
        - DataFrame
        - dict[Any, DataFrame]

        This could be extended to include other parameterized generic types
        """

        if type(other) is not type(self):
            return False

        field_equalities = []

        for field in self.model_fields:
            self_field = getattr(self, field)
            other_field = getattr(other, field)

            field_type = self.model_fields[field]
            if field_type.annotation is pd.DataFrame:
                val = self_field.equals(other_field)
            elif (
                typing.get_origin(field_type.annotation) is dict
                and typing.get_args(field_type.annotation)[1] is pd.DataFrame
            ):
                if self_field.keys() != other_field.keys():
                    val = False
                else:
                    pd_result = [
                        self_field[key].equals(other_field[key]) for key in self_field
                    ]
                    val = all(pd_result)

            else:
                val = self_field == other_field

            field_equalities.append(val)

        return all(field_equalities)
