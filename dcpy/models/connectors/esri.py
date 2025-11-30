from enum import StrEnum

from pydantic import BaseModel


class Server(StrEnum):
    dcp = "dcp"
    nps = "nps"
    nyc_maphub = "nyc_maphub"
    nys_clearinghouse = "nys_clearinghouse"
    nys_parks = "nys_parks"
    us_fws = "us_fws"


servers = {
    "dcp": {"id": "GfwWNkhOj9bNBqoJ", "subdomain": "services5"},
    "nps": {"id": "fBc8EJBxQRMcHlei", "subdomain": "services1"},
    "nyc_maphub": {"id": "yG5s3afENB5iO9fj", "subdomain": "services6"},
    "nys_clearinghouse": {"id": "DZHaqZm9cxOD4CWM", "subdomain": "services6"},
    "nys_parks": {"id": "1xFZPtKn1wKC6POA"},
    "us_fws": {"id": "QVENGdaPbd4LUkLV"},
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
