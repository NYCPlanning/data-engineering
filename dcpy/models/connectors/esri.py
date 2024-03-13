from enum import StrEnum
from pydantic import BaseModel


class Server(StrEnum):
    nys_clearinghouse = "nys_clearinghouse"
    nys_parks = "nys_parks"
    nps = "nps"
    dcp = "dcp"


servers = {
    "nys_clearinghouse": {"id": "DZHaqZm9cxOD4CWM", "subdomain": "services6"},
    "nys_parks": {"id": "1xFZPtKn1wKC6POA"},
    "nps": {"id": "fBc8EJBxQRMcHlei", "subdomain": "services1"},
    "dcp": {"id": "GfwWNkhOj9bNBqoJ", "subdomain": "services5"},
}


class FeatureServer(BaseModel, extra="forbid", use_enum_values=True):
    server: Server
    name: str
    layer: int = 0

    @property
    def url(self):
        subdomain = servers[self.server].get("subdomain", "services")
        server_id = servers[self.server]["id"]
        return f"https://{subdomain}.arcgis.com/{server_id}/ArcGIS/rest/services/{self.name}/FeatureServer/{self.layer}"
