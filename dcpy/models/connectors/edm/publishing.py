from abc import ABC, abstractmethod
from dataclasses import dataclass


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
    version: str
    revision: str

    def __str__(self):
        return f"Draft: {self.product} - {self.version} ({self.revision})"

    @property
    def path(self) -> str:
        return f"{self.product}/draft/{self.version}/{self.revision}"


@dataclass
class BuildKey(ProductKey):
    product: str
    build: str

    def __str__(self):
        return f"Build: {self.product} - {self.build}"

    @property
    def path(self) -> str:
        return f"{self.product}/build/{self.build}"
