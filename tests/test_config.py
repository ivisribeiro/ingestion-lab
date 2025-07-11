from ingestion.config import Config
import os
import tempfile
import json
import yaml

def test_load_json():
    cfg_data = {"a": 1}
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(cfg_data, f)
        path = f.name
    cfg = Config.from_file(path)
    assert cfg.get("a") == 1


def test_load_yaml():
    cfg_data = {"b": 2}
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
        yaml.dump(cfg_data, f)
        path = f.name
    cfg = Config.from_file(path)
    assert cfg.get("b") == 2


def test_from_env(monkeypatch):
    monkeypatch.setenv("INGESTION_TEST", "value")
    cfg = Config.from_env()
    assert cfg.get("test") == "value"
