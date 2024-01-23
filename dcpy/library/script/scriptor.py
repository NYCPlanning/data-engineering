class ScriptorInterface:
    config: dict

    def __init__(self, config: dict):
        self.config = config

    @property
    def name(self) -> str:
        return self.config["name"]

    @property
    def version(self) -> str:
        return self.config["version"]

    @property
    def path(self) -> str:
        return self.config["source"]["script"]["path"]
