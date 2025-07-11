from pyspark.sql import SparkSession
from typing import Dict


def gcs_spark(app_name: str = "IngestionGCS", options: Dict[str, str] | None = None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    if options:
        for k, v in options.items():
            builder = builder.config(k, v)
    # Example GCS connector config
    if "gcs_keyfile" in options:
        builder = builder.config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        builder = builder.config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", options["gcs_keyfile"])
    return builder.getOrCreate()
