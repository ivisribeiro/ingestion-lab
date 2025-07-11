# ingestion-lab

This project contains a simple data ingestion framework using Spark.

## Setup

Install dependencies:

```bash
pip install -r requirements.txt
```

## Running

Create a configuration file in YAML or JSON specifying source and destination
settings. Example:

```yaml
destination: minio
source_format: csv
source_path: data/input.csv
dest_format: parquet
dest_path: s3a://bucket/output/
```

Run the ingestion job:

```bash
python -m ingestion.cli --config config.yaml
```

## Testing

Run unit tests with `pytest`:

```bash
pytest
```
