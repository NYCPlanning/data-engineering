from pydantic import BaseModel
from typing import Literal


class FileDownloadSource(BaseModel, extra="forbid"):
    type: Literal["file_download"]
    url: str


class GenericApiSource(BaseModel, extra="forbid"):
    type: Literal["api"]
    endpoint: str
    format: Literal["json", "csv"]
