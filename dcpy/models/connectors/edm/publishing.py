from abc import ABC, abstractmethod
from dataclasses import dataclass

from pydantic import BaseModel
from typing import Literal


class ProductKey(ABC):
    product: str

    @property
    @abstractmethod
    def path(self) -> str:
        raise NotImplementedError("ProductKey is an abstract class")


@dataclass
class PublishKey(ProductKey):
    product: str
    version: str

    def __str__(self):
        return f"{self.product} - {self.version}"

    @property
    def path(self) -> str:
        return f"{self.product}/publish/{self.version}"


@dataclass
class DraftKey(ProductKey):
    product: str
    build: str

    def __str__(self):
        return f"Draft: {self.product} - {self.build}"

    @property
    def path(self) -> str:
        return f"{self.product}/draft/{self.build}"


@dataclass
class BuildKey(ProductKey):
    product: str
    build: str

    def __str__(self):
        return f"Build: {self.product} - {self.build}"

    @property
    def path(self) -> str:
        return f"{self.product}/build/{self.build}"


class GisDataset(BaseModel, extra="forbid"):
    """Dataset published by GIS in edm-publishing/datasets"""

    # Some datasets here will phased out if we eventually get data
    # directly from GR or other sources
    type: Literal["edm_publishing_gis_dataset"]
    name: str
