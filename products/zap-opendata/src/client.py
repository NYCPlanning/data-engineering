from functools import cached_property

import msal


class Client:
    def __init__(self, zap_domain: str, tenant_id: str, client_id: str, secret: str):
        self.config = {
            "authority": f"https://login.microsoftonline.com/{tenant_id}",
            "client_id": client_id,
            "scope": [f"{zap_domain}/.default"],
            "secret": secret,
            "endpoint": f"{zap_domain}/api/data/v9.1",
        }
        self.app = msal.ConfidentialClientApplication(
            self.config["client_id"],
            authority=self.config["authority"],
            client_credential=self.config["secret"],
        )

    @cached_property
    def access_token(self) -> str:
        result = self.app.acquire_token_for_client(scopes=self.config["scope"])
        if list(result.keys()) == ["error"]:
            raise PermissionError(result)
        return result["access_token"]

    @cached_property
    def request_header(self) -> str:
        return {"Authorization": "Bearer " + self.access_token}
