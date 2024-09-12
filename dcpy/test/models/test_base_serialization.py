from typing import Literal

from dcpy.models.base import SortedSerializedBase


class MySortedBaseClass(SortedSerializedBase):
    _head_sort_order = ["z_put_me_first"]
    _tail_sort_order = ["z_put_me_last"]
    _exclude_falsey_values = False


class ChildToBeSerialized(MySortedBaseClass):
    # Fully defining the order here for ease of testing
    _head_sort_order = ["b", "a", "c"]

    a: str | None = None
    b: str | None = None
    c: str | None = None


class ToBeSerialized(MySortedBaseClass):
    z_put_me_first: list[str]
    z_put_me_last: str

    # Complex Values
    f: list[str]
    g: dict[str, str]

    # Simple values. (disordered, to make sure tests don't pass by default)
    c: str
    b: int | None
    a: int
    e: Literal["z"]
    d: str | None

    _private_unserialized_field = "I shouldn't show up in outputs"

    child_to_be_serialized: ChildToBeSerialized


ordered_args = {
    "z_put_me_last": "hi",
    "z_put_me_first": ["hi"],
}
simple_args = {
    "a": 0,
    "b": None,
    "c": "hi",
    "d": None,
    "e": "z",
}
complex_args = {
    "f": ["hi"],
    "g": {"hi": "hi"},
}


def test_dump_prune_falsey_vals():
    """Construct a model where the child model is composed entirely of None values.
    The child model should serialize to {} due to built-in `exclude_none=True`,
    and {} should then be pruned from the serialization."""
    model = ToBeSerialized(
        child_to_be_serialized=ChildToBeSerialized(),  # should be pruned
        **(ordered_args | simple_args | complex_args),
    )
    model._exclude_falsey_values = True
    dumped = model.model_dump(exclude_none=True)
    assert (
        "child_to_be_serialized" not in dumped
    ), "Falsey values should have been excluded from serialization."
    assert (
        "a" in dumped
    ), "The falsey value of 0 should not have been excluded from the model"


def test_dumping():
    """Tests serializing of simple and complex fields on a subclass, where the sort_order overrides
    are defined on the parent class. Also tests that model serializing is recursive (ie child classes also sort correctly).
    """
    model = ToBeSerialized(
        child_to_be_serialized=ChildToBeSerialized(a="a", b="b", c="c"),
        **(ordered_args | simple_args | complex_args),
    )

    expected_key_order = (
        model._head_sort_order
        + sorted(list(simple_args.keys()))
        + sorted(["child_to_be_serialized"] + list(complex_args.keys()))
        + model._tail_sort_order
    )

    dumped = model.model_dump()

    assert (
        list(dumped.keys()) == expected_key_order
    ), "The model should serialize with keys in the correct order."

    assert model.child_to_be_serialized._head_sort_order == list(
        dumped["child_to_be_serialized"].keys()
    ), "Child objects should also deserialize in order."
