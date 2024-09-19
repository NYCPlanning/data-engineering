from enum import StrEnum
from pydantic import BaseModel


class Server(StrEnum):
    nys_clearinghouse = "nys_clearinghouse"
    nys_parks = "nys_parks"
    nps = "nps"
    dcp = "dcp"
    nyc_maphub = "nyc_maphub"


servers = {
    "nys_clearinghouse": {"id": "DZHaqZm9cxOD4CWM", "subdomain": "services6"},
    "nys_parks": {"id": "1xFZPtKn1wKC6POA"},
    "nps": {"id": "fBc8EJBxQRMcHlei", "subdomain": "services1"},
    "dcp": {"id": "GfwWNkhOj9bNBqoJ", "subdomain": "services5"},
    "nyc_maphub": {"id": "yG5s3afENB5iO9fj", "subdomain": "services6"},
}


class FeatureServer(BaseModel, extra="forbid"):
    server: Server
    name: str

    @property
    def url(self) -> str:
        subdomain = servers[self.server].get("subdomain", "services")
        server_id = servers[self.server]["id"]
        return f"https://{subdomain}.arcgis.com/{server_id}/ArcGIS/rest/services/{self.name}/FeatureServer"


class FeatureServerLayer(BaseModel, extra="forbid"):
    server: Server
    name: str
    layer_name: str
    layer_id: int

    @property
    def feature_server(self) -> FeatureServer:
        return FeatureServer(server=self.server, name=self.name)

    @property
    def url(self) -> str:
        return f"{self.feature_server.url}/{self.layer_id}"

    @property
    def layer_label(self) -> str:
        return f"{self.layer_name} ({self.layer_id})"
