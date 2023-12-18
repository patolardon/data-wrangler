from abc import ABC
import json


class JsonConfig(ABC):
    def __init__(self, config_path):
        self.json_config_path = f"{config_path}/{type(self).__name__}.json"

    @property
    def config(self):
        with open(self.json_config_path) as f:
            config = json.load(f)
        return config
