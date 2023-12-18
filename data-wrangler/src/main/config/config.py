from config.jsonConfig.exportConfig import ExportConfig
from config.jsonConfig.sourceConfig import SourceConfig


class Config:
    def __init__(self,
                 env: str,
                 destination: str,
                 config_path="../resources"):
        self.source_config = SourceConfig(env=env, config_path=config_path)
        self.export_config = ExportConfig(config_path, destination)
