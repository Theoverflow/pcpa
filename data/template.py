import argparse
import importlib
import json
import logging
import sys
import signal
import atexit
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_EXCEPTION
from datetime import datetime
from threading import Event
from pydantic import BaseModel, Field, ValidationError, root_validator

# Graceful shutdown event
shutdown_event = Event()

class LibraryConfig(BaseModel):
    name: str = Field(..., description="Library package base name")
    version: str = Field(..., description="Version string, e.g. '1.2.3'")

class TaskConfig(BaseModel):
    id: str
    params: dict = Field(default_factory=dict)
    expected_value: float = Field(..., description="Business value weight for the task")

class AppConfig(BaseModel):
    threads: int = Field(4, gt=0, description="Number of threads for parallel compute")
    libraries: list[LibraryConfig]
    tasks: list[TaskConfig]

    @root_validator()
    def check_unique_libraries_and_tasks(cls, values):
        libs = values.get('libraries', [])
        tasks = values.get('tasks', [])
        if len({lib.name for lib in libs}) != len(libs):
            raise ValueError("Duplicate library names in configuration")
        if len({task.id for task in tasks}) != len(tasks):
            raise ValueError("Duplicate task IDs in configuration")
        return values

class GeneratorErrorHandler:
    """
    Wraps a generator to catch and handle exceptions gracefully.
    Optionally updates metrics on failure.
    """
    def __init__(self, generator, logger=None, metrics=None, handler_name=None):
        self._gen = generator
        self.logger = logger or logging
        self.metrics = metrics
        self.name = handler_name or getattr(generator, '__name__', 'generator')

    def __iter__(self):
        return self

    def __next__(self):
        if shutdown_event.is_set():
            raise StopIteration
        try:
            return next(self._gen)
        except StopIteration:
            raise
        except Exception as e:
            self.logger.exception("Error in generator '%s': %s", self.name, e)
            if self.metrics is not None:
                self.metrics.setdefault('generator_errors', 0)
                self.metrics['generator_errors'] += 1
            raise

def setup_logging(logfile: str):
    fmt = '[%(asctime)s] %(levelname)s %(threadName)s %(name)s: %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.FileHandler(logfile),
            logging.StreamHandler(sys.stdout)
        ]
    )

def load_precompiled_library(lib_cfg: LibraryConfig):
    module_name = f"{lib_cfg.name}_v{lib_cfg.version.replace('.', '_')}"
    try:
        module = importlib.import_module(module_name)
        logging.info("Loaded library: %s (module %s)", lib_cfg.name, module_name)
        return module
    except ImportError:
        logging.exception("Failed to load library %s version %s", lib_cfg.name, lib_cfg.version)
        raise

def trace(func):
    def wrapper(*args, **kwargs):
        start = datetime.utcnow()
        logging.debug("Entering %s", func.__name__)
        try:
            return func(*args, **kwargs)
        finally:
            duration = (datetime.utcnow() - start).total_seconds()
            logging.debug("Exiting %s, duration=%0.4fs", func.__name__, duration)
    return wrapper

@trace
def process_task(task: TaskConfig, libs: dict, metrics: dict):
    try:
        start = datetime.utcnow()
        lib = libs.get(task.params.get('lib_name'))
        if not lib:
            raise RuntimeError(f"Library {task.params.get('lib_name')} not configured")
        result = lib.compute(**task.params)
        duration = (datetime.utcnow() - start).total_seconds()
        metrics['task_count'] += 1
        metrics['success_count'] += 1
        metrics['durations'].append(duration)
        metrics['business_value'] += task.expected_value
        logging.info("Task %s succeeded in %0.3fs, value=%0.2f", task.id, duration, task.expected_value)
        return task.id, result
    except Exception:
        metrics['task_count'] += 1
        metrics['failure_count'] += 1
        logging.exception("Task %s failed", task.id)
        return task.id, None

def cleanup():
    logging.info("Running cleanup before exit...")
    # Add resource cleanup here (DB, files, threads)
    logging.info("Cleanup complete.")

def signal_handler(signum, frame):
    logging.warning("Received signal %d, shutting down.", signum)
    shutdown_event.set()

def main():
    # Register signals and atexit cleanup
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    atexit.register(cleanup)

    parser = argparse.ArgumentParser(description="Mission-critical one-shot processor with graceful exit")
    parser.add_argument('config', help="Path to JSON configuration file")
    parser.add_argument('--log', default='processor.log', help="Log file path")
    args = parser.parse_args()

    setup_logging(args.log)

    try:
        with open(args.config) as f:
            cfg = AppConfig.parse_obj(json.load(f))
    except (ValidationError, ValueError, json.JSONDecodeError):
        logging.exception("Invalid configuration -- aborting")
        sys.exit(1)

    libs = {lib.name: load_precompiled_library(lib) for lib in cfg.libraries}

    metrics = {
        'task_count': 0,
        'success_count': 0,
        'failure_count': 0,
        'durations': [],
        'business_value': 0.0
    }

    def task_generator(tasks):
        for t in tasks:
            yield t

    gen = GeneratorErrorHandler(task_generator(cfg.tasks), logger=logging, metrics=metrics)

    try:
        with ThreadPoolExecutor(max_workers=cfg.threads) as executor:
            futures = []
            for task in gen:
                if shutdown_event.is_set():
                    break
                futures.append(executor.submit(process_task, task, libs, metrics))
            # Wait for running tasks, but stop early on error or shutdown
            done, not_done = wait(futures, return_when=FIRST_EXCEPTION)
            if shutdown_event.is_set():
                logging.info("Shutdown requested, cancelling remaining tasks.")
                for f in not_done:
                    f.cancel()
    except Exception:
        logging.exception("Fatal error during processing")
        sys.exit(1)
    finally:
        # Summary and cleanup
        total = metrics['task_count']
        succ = metrics['success_count']
        fail = metrics['failure_count']
        avg_time = sum(metrics['durations']) / succ if succ else 0
        logging.info(
            "Completed %d tasks: %d success, %d failure, avg time=%.3fs, total value=%.2f, generator errors=%d",
            total, succ, fail, avg_time, metrics['business_value'], metrics.get('generator_errors', 0)
        )
        cleanup()
        sys.exit(1 if fail or shutdown_event.is_set() else 0)

if __name__ == '__main__':
    main()
