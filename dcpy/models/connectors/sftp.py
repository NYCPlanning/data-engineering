from pydantic import BaseModel


class SFTPServer(BaseModel, extra="forbid"):
    hostname: str
    port: int


class SFTPUser(BaseModel, extra="forbid"):
    username: str
