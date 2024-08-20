from pydantic import BaseModel, model_serializer
from pydantic.fields import PrivateAttr


# BASE
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
