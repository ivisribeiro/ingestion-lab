import os
import json
from pathlib import Path
from typing import Any, Dict

import yaml


class Config:
    """Load configuration from YAML/JSON files or environment variables."""

    def __init__(self, data: Dict[str, Any]):
        self.data = data

    @classmethod
    def from_file(cls, path: str) -> "Config":
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Config file {path} not found")
        if p.suffix in {".yaml", ".yml"}:
            with p.open() as f:
                data = yaml.safe_load(f)
        elif p.suffix == ".json":
            with p.open() as f:
                data = json.load(f)
        else:
            raise ValueError("Unsupported config file format")
        return cls(data)

    @classmethod
    def from_env(cls, prefix: str = "INGESTION_") -> "Config":
        data = {k[len(prefix):].lower(): v for k, v in os.environ.items() if k.startswith(prefix)}
        return cls(data)

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)
