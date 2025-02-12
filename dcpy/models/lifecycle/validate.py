from typing import TypedDict, NotRequired


class ValidationArgs(TypedDict):
    ignore_validation_errors: NotRequired[bool]
    skip_validation: NotRequired[bool]
