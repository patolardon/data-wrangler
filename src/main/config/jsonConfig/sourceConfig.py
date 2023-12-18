from config.jsonConfig.jsonConfig import JsonConfig
from readers.csvReader import CsvReader
from readers.reader import Reader
from readers.tableReader import TableReader


class SourceConfigError(Exception):
    pass


class Source:
    def __init__(self, name, path, reader_type, reader_options):
        self.name = name
        self.path = path
        self.reader_options = reader_options
        try:
            self.reader: Reader = {"csv": CsvReader,
                                   "table": TableReader}[reader_type]
        except KeyError:
            raise SourceConfigError(f"reader type {reader_type} is not a valid key")

    def __repr__(self):
        return f'{self.name}: {self.path} read with {self.reader}'


class SourceConfig(JsonConfig):
    def __init__(self, env, config_path):
        super().__init__(config_path)
        self.env = env

    def __repr__(self):
        return f'Configuration class for {self.env}'

    @property
    def sources(self) -> [Source]:
        return [
            Source(name, source[name]["location"][self.env], key, source[name].get("options", {}))
            for key in self.config
            for source in self.config[key]
            for name in source
        ]


