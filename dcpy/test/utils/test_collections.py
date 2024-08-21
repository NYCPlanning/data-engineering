from dcpy.utils import collections


def test_deep_merge():
    assert collections.deep_merge_dict(
        {"a": "1", "b": {"c": {"d": "e"}}},
        {"a": "1", "b": {"c": {"x": "y"}}},
    ) == {"a": "1", "b": {"c": {"d": "e", "x": "y"}}}
