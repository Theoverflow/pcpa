import pytest
from pydantic import ValidationError
from src.processor import AppConfig, LibraryConfig, TaskConfig

def test_valid_config():
    cfg = AppConfig(
        threads=1, 
        libraries=[LibraryConfig(name="lib", version="1.0")],
        tasks=[TaskConfig(id="t1", params={}, expected_value=0.5)]
    )
    assert cfg.threads == 1

def test_duplicate_library_names():
    libs = [LibraryConfig(name="dup", version="1.0"), LibraryConfig(name="dup", version="2.0")]
    with pytest.raises(ValidationError):
        AppConfig(threads=1, libraries=libs, tasks=[])

def test_duplicate_task_ids():
    tasks = [TaskConfig(id="t1", params={}, expected_value=0.1), TaskConfig(id="t1", params={}, expected_value=0.2)]
    with pytest.raises(ValidationError):
        AppConfig(threads=1, libraries=[], tasks=tasks)
