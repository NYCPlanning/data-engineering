from pydantic import BaseModel


class SFTPServer(BaseModel, extra="forbid"):
    hostname: str
    port: int = 22


class SFTPUser(BaseModel, extra="forbid"):
    username: str
    private_key_name: str | None = None
