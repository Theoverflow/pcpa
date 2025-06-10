import pytest
import subprocess
import sys

def test_full_run(tmp_path, tmp_config_file):
    # Run the main script as a subprocess
    res = subprocess.run(
        [sys.executable, "-u", "src/processor.py", tmp_config_file, "--log", str(tmp_path / "out.log")],
        cwd=str(Path(__file__).parents[1]),
        capture_output=True,
        text=True
    )
    # exit code 0 => success (no failures or shutdown)
    assert res.returncode == 0
    # log file should exist and contain summary
    log = (tmp_path / "out.log").read_text()
    assert "Completed" in log and "success" in log
