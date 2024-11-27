from typing import Any
from pydantic import BaseModel, Field

from dcpy.utils.string import to_snake, to_camel


def pydantic_class_from_dict(class_name: str, obj: dict[str, Any]) -> str:
    str = f"class {class_name}(BaseModel):"
    subclasses = []

    for field in obj:
        str += "\n    "
        val = obj[field]
        if isinstance(val, dict):
            type_name = to_camel(field)
            subclasses.append(pydantic_class_from_dict(type_name, val))
        elif isinstance(val, list) and (len(val) > 0):
            class_name = to_camel(field)
            type_name = f"list[{class_name}]"
            subclasses.append(pydantic_class_from_dict(class_name, val[0]))
        else:
            type_name = type(val).__name__
        field_name = to_snake(field)
        if field != field_name:
            str += f'{field_name}: {type_name} = Field(alias="{field}")'
        else:
            str += f"{field_name}: {type_name}"

    subclasses.append(str)
    return "\n\n".join(subclasses)
