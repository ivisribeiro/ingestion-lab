import argparse

from ingestion import config
from ingestion.pipeline import IngestionPipeline
from ingestion.connections import jdbc, minio, gcs


def main() -> None:
    parser = argparse.ArgumentParser(description="Simple ingestion CLI")
    parser.add_argument("--config", help="Path to config file")
    args = parser.parse_args()

    cfg = config.Config.from_file(args.config)
    dest = cfg.get("destination")
    if dest == "jdbc":
        spark = jdbc.jdbc_spark(options=cfg.get("spark", {}))
    elif dest == "minio":
        spark = minio.minio_spark(options=cfg.get("spark", {}))
    elif dest == "gcs":
        spark = gcs.gcs_spark(options=cfg.get("spark", {}))
    else:
        spark = jdbc.jdbc_spark()

    pipeline = IngestionPipeline(spark)
    pipeline.run(
        cfg.get("source_format"),
        cfg.get("source_path"),
        cfg.get("dest_format"),
        cfg.get("dest_path"),
        cfg.get("source_options", {}),
        cfg.get("dest_options", {}),
    )


if __name__ == "__main__":
    main()
