from typing import NotRequired, TypedDict


class ValidationArgs(TypedDict):
    ignore_validation_errors: NotRequired[bool]
    skip_validation: NotRequired[bool]
