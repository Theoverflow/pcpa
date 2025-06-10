import pytest
from src.processor import GeneratorErrorHandler, shutdown_event

def broken_gen():
    yield 1
    raise RuntimeError("boom")

def test_generator_error_counts(monkeypatch):
    metrics = {}
    handler = GeneratorErrorHandler(broken_gen(), metrics=metrics)
    it = iter(handler)
    assert next(it) == 1
    with pytest.raises(RuntimeError):
        next(it)
    # ensure error was logged in metrics
    assert metrics.get('generator_errors', 0) == 1

def test_shutdown_stops_iteration():
    shutdown_event.set()
    handler = GeneratorErrorHandler(iter([1,2,3]))
    with pytest.raises(StopIteration):
        next(iter(handler))
    shutdown_event.clear()
