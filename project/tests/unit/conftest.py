import json
import tempfile
import os
import pytest
from src.processor import AppConfig, TaskConfig, LibraryConfig, process_task, GeneratorErrorHandler

@pytest.fixture
def sample_lib_module(monkeypatch):
    class DummyLib:
        def compute(self, **params):
            return sum(params.values())
    return DummyLib()

@pytest.fixture
def sample_task():
    return TaskConfig(id="t1", params={"a": 1.0, "b": 2.0, "lib_name": "dummy"}, expected_value=5.0)

@pytest.fixture
def metrics_dict():
    return {'task_count':0,'success_count':0,'failure_count':0,'durations':[],'business_value':0.0}

@pytest.fixture
def tmp_config_file(tmp_path):
    cfg = {
        "threads": 2,
        "libraries": [{"name": "dummy", "version": "0.1"}],
        "tasks": [{"id": "t1", "params": {"lib_name": "dummy", "x": 3.0}, "expected_value": 1.0}]
    }
    f = tmp_path / "cfg.json"
    f.write_text(json.dumps(cfg))
    return str(f)
