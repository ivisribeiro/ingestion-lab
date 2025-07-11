from pyspark.sql import SparkSession
from typing import Dict


def minio_spark(app_name: str = "IngestionMinIO", options: Dict[str, str] | None = None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    if options:
        for k, v in options.items():
            builder = builder.config(k, v)
    # Example S3A endpoint configuration
    builder = builder.config("spark.hadoop.fs.s3a.endpoint", options.get("endpoint"))
    builder = builder.config("spark.hadoop.fs.s3a.access.key", options.get("access_key"))
    builder = builder.config("spark.hadoop.fs.s3a.secret.key", options.get("secret_key"))
    builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
    return builder.getOrCreate()
