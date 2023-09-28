class ScriptorInterface:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    @property
    def version(self):
        return self.config["dataset"]["version"]

    @property
    def path(self):
        return self.config["dataset"]["source"]["path"]
