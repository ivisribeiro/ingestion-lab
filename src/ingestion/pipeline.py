from pyspark.sql import DataFrame, SparkSession
from typing import Dict


class IngestionPipeline:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_source(self, fmt: str, path: str, options: Dict[str, str] | None = None) -> DataFrame:
        reader = self.spark.read
        if options:
            reader = reader.options(**options)
        if fmt == "jdbc":
            return reader.format("jdbc").load(path)
        return reader.format(fmt).load(path)

    def write_destination(self, df: DataFrame, fmt: str, path: str, mode: str = "overwrite", options: Dict[str, str] | None = None) -> None:
        writer = df.write.mode(mode)
        if options:
            writer = writer.options(**options)
        if fmt == "jdbc":
            writer.format("jdbc").save(path)
        else:
            writer.format(fmt).save(path)

    def run(self, src_fmt: str, src_path: str, dest_fmt: str, dest_path: str, src_options: Dict[str, str] | None = None, dest_options: Dict[str, str] | None = None):
        df = self.read_source(src_fmt, src_path, src_options)
        self.write_destination(df, dest_fmt, dest_path, options=dest_options)
