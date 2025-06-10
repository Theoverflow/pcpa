import pytest
from src.processor import process_task

class FakeLib:
    def compute(self, **p): 
        if p.get('fail'): raise ValueError("bad")
        return 42

def test_process_task_success(metrics_dict):
    task = pytest.TaskConfig(id="t1", params={"lib_name":"f", "x":1.0}, expected_value=2.0)
    libs = {"f": FakeLib()}
    tid, res = process_task(task, libs, metrics_dict)
    assert tid == "t1" and res == 42
    assert metrics_dict['success_count'] == 1

def test_process_task_failure(metrics_dict):
    task = pytest.TaskConfig(id="t2", params={"lib_name":"f", "fail":True}, expected_value=2.0)
    libs = {"f": FakeLib()}
    tid, res = process_task(task, libs, metrics_dict)
    assert res is None
    assert metrics_dict['failure_count'] == 1
