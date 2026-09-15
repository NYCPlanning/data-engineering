from typing import Any

from pandas import DataFrame


def deep_merge_dict(dict1, dict2):
    """
    Recursively merge two dictionaries.

    Args:
        dict1 (dict): The first dictionary to merge.
        dict2 (dict): The second dictionary to merge.

    Returns:
        dict: A new dictionary with the merged contents of dict1 and dict2.
    """
    result = dict1.copy()
    for key, value in dict2.items():
        if isinstance(value, dict) and key in result and isinstance(result[key], dict):
            result[key] = deep_merge_dict(result[key], value)
        else:
            result[key] = value
    return result


def flatten_and_indent(
    obj: dict | list,
    *,
    indent: str = "    ",
    offset: str = "",
    max_df_length: int = 20,
    include_line_breaks: bool = False,
    pretty_print_fields: bool = False,
    max_recursion_depth: int = 10,
) -> list[str]:
    """
    Recursively prints a dictionary or list, indenting as the nested structure goes deeper.
    For pydantic models, call `model_dump()` to feed it to this function
    """

    class Pair:
        """Simple class to represent single key, value pair to iterate through dicts and avoid "collisions" with behavior for other types"""

        key: str
        value: Any

        def __init__(self, key, value):
            self.key = key
            self.value = value

    def falsey_none(el):
        if not isinstance(el, DataFrame) and not el and el != 0:
            return None
        else:
            return el

    def apply_indent(el, level):
        i = offset + indent * level
        return i + str(el)

    def df_to_list(df: DataFrame):
        if len(df) > max_df_length:
            return [f"{len(df)} rows. First {max_df_length} shown"] + df.head(
                max_df_length
            ).to_string().split("\n")
        else:
            return df.to_string().split("\n")

    def flatten(li: list | dict, level: int) -> list:
        if level > max_recursion_depth:
            return [apply_indent("[Truncated - max depth exceeded]", level)]
        if isinstance(li, dict):
            return flatten([Pair(key=k, value=li[k]) for k in li], level)

        if li == []:
            return li

        current_node = li[0]

        if isinstance(current_node, list) or isinstance(current_node, dict):
            first = flatten(current_node, level + 1)

        elif isinstance(current_node, set):
            first = flatten(sorted(list(current_node)), level + 1)

        elif isinstance(current_node, DataFrame):
            first = flatten(df_to_list(current_node), level)

        elif isinstance(current_node, Pair):
            key = current_node.key
            if pretty_print_fields:
                key = key.replace("_", " ").capitalize()
            value = falsey_none(current_node.value)
            if (
                isinstance(value, str)
                or isinstance(value, int)
                or isinstance(value, float)
                or isinstance(value, bool)
                or value is None
            ):
                first = [apply_indent(f"{key}: {value}", level)]
            elif isinstance(value, dict) or isinstance(value, list):
                first = [apply_indent(key, level)] + flatten(value, level + 1)
            elif isinstance(value, set):
                first = [apply_indent(key, level)] + flatten(
                    sorted(list(value)), level + 1
                )
            else:
                first = [apply_indent(key, level)] + flatten(
                    [current_node.value], level + 1
                )

        else:
            first = [apply_indent(current_node, level)]

        if include_line_breaks and level == 0:
            first = [("_" * 80)] + first

        return first + flatten(li[1:], level)

    return flatten(obj, level=0)


def indented_report(
    obj: dict | list,
    *,
    indent: str = "    ",
    offset: str = "",
    max_df_length: int = 20,
    include_line_breaks: bool = False,
    pretty_print_fields: bool = False,
) -> str:
    return "\n".join(
        flatten_and_indent(
            obj,
            indent=indent,
            offset=offset,
            max_df_length=max_df_length,
            include_line_breaks=include_line_breaks,
            pretty_print_fields=pretty_print_fields,
        )
    )
