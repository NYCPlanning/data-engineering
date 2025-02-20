from dcpy.models.base import SortedSerializedBase, YamlWriter, TemplatedYamlReader

from enum import Enum
from typing import Optional
from pydantic import Field


class StepType(Enum):
    PULL_FILE = "pull_file"
    DISASSEMBLE = "disassemble"
    GENERATE_ARTIFACT = "generate_artifact"


class Step(SortedSerializedBase):
    type: StepType
    source: str
    targets: list[str]
    description: Optional[str] = None
    custom: Optional[dict] = Field(default_factory=dict)


class Script(SortedSerializedBase):
    name: str
    description: Optional[str] = None
    steps: list[Step]
    metadata: Optional[dict] = Field(default_factory=dict)
