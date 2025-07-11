from pyspark.sql import SparkSession
from typing import Dict


def jdbc_spark(app_name: str = "IngestionJDBC", options: Dict[str, str] | None = None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    if options:
        for k, v in options.items():
            builder = builder.config(k, v)
    return builder.getOrCreate()
