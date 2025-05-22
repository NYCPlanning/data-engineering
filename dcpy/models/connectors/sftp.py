from pydantic import BaseModel


class SFTPServer(BaseModel, extra="forbid"):
    hostname: str
    port: int = 22


class SFTPUser(BaseModel, extra="forbid"):
    username: str
    private_key_path: str | None = None
    known_hosts_path: str = "~/.ssh/known_hosts"
