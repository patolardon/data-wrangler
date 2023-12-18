from exporter.Exporter import Exporter


class AddExporter(Exporter):
    def export(self) -> None:
        (self.input_dataset.write.mode("overwrite")
         .option("path", self.export_config.destination_path)
         .saveAsTable(self.export_config.destination, format="csv")
         )
