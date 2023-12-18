from config.jsonConfig.jsonConfig import JsonConfig


class ExportConfigError(Exception):
    pass


class ExportConfig(JsonConfig):
    def __init__(self, config_path, destination):
        super().__init__(config_path=config_path)
        self.destination = destination
        self.destination_path = destination.replace(".", "/")
        try:
            self.export_type = self.config["export_type"]
        except KeyError:
            raise ExportConfigError(f"export type must be found in 'export_type' path in {type(self).__name__}.json")

