class ScriptorInterface:
    config: dict

    def __init__(self, config: dict):
        self.config = config

    @property
    def name(self) -> str:
        return self.config["dataset"]["name"]

    @property
    def version(self) -> str:
        return self.config["dataset"]["version"]

    @property
    def path(self) -> str:
        return self.config["dataset"]["source"]["path"]
