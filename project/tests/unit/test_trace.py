import logging
from src.processor import trace

@trace
def dummy(x, y):
    return x + y

def test_trace_decorator_logs(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    result = dummy(2,3)
    assert result == 5
    # ensure entry and exit logged
    assert any("Entering dummy" in r.message for r in caplog.records)
    assert any("Exiting dummy" in r.message for r in caplog.records)
