from typing import Any
from pydantic import BaseModel


class Field(BaseModel):
    name: str
    data_type: str | None = None  # should probably be enum
    checks: dict[str, Any] | None = None
    required: bool = True


class Fields(BaseModel):
    fields: list[Field]
