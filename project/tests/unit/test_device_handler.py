# File: tests/unit/test_device_tester.py
import pandas as pd
import numpy as np
import pytest
import tempfile
import os
from src.device_tester import DeviceTester

@pytest.fixture
def sample_csvs(tmp_path):
    # Create devices CSV
    devices = pd.DataFrame({
        'current_status': [True, True, False],
        'test_A': [1.0, 5.0, 10.0],
        'test_B': [2.0, 0.0, -1.0]
    }, index=['dev1', 'dev2', 'dev3'])
    limits = pd.DataFrame({
        'lower_limit': [0.5, 1.5],
        'upper_limit': [3.0, 6.0]
    }, index=['test_A', 'test_B'])

    dev_file = tmp_path / "devices.csv"
    lim_file = tmp_path / "limits.csv"
    devices.to_csv(dev_file)
    limits.to_csv(lim_file)
    return str(dev_file), str(lim_file), devices, limits

def test_load_data_success(sample_csvs):
    dev_path, lim_path, devices, limits = sample_csvs
    tester = DeviceTester(dev_path, lim_path)
    tester.load_data()
    # DataFrames loaded correctly
    pd.testing.assert_frame_equal(tester.df, devices)
    pd.testing.assert_frame_equal(tester.limits, limits)
    # Test names and arrays
    assert tester.test_names == ['test_A', 'test_B']
    np.testing.assert_array_equal(tester.df_values_np, devices[['test_A','test_B']].to_numpy())
    np.testing.assert_array_equal(tester.lower_arr, np.array([0.5, 1.5]))
    np.testing.assert_array_equal(tester.upper_arr, np.array([3.0, 6.0]))
    np.testing.assert_array_equal(tester.current_status, np.array([True, True, False]))

def test_load_data_no_tests(tmp_path):
    # Devices with no test_ columns
    df = pd.DataFrame({'foo': [1,2]}, index=['a','b'])
    limits = pd.DataFrame({'lower_limit':[0], 'upper_limit':[10]}, index=['foo'])
    dev_file = tmp_path / "devices.csv"
    lim_file = tmp_path / "limits.csv"
    df.to_csv(dev_file)
    limits.to_csv(lim_file)
    tester = DeviceTester(str(dev_file), str(lim_file))
    with pytest.raises(RuntimeError):
        tester.load_data()

@pytest.mark.parametrize("arr,lower,upper,expected", [
    (np.array([[1,2],[0,5]]), np.array([0,1]), np.array([2,5]), np.array([[True,True],[True,True]])),
    (np.array([[3,6]]), np.array([0,5]), np.array([5,10]), np.array([[True,True]])),
])
def test_mask_within_bounds(arr, lower, upper, expected):
    mask = DeviceTester.mask_within_bounds(arr, lower, upper)
    np.testing.assert_array_equal(mask, expected)

@pytest.mark.parametrize("mask, status, expected", [
    (np.array([[True,True],[True,False]]), np.array([True, True]), np.array([True, False])),
    (np.array([[True,True],[False,False]]), np.array([False, True]), np.array([False, False])),
])
def test_compute_post_status(mask, status, expected):
    result = DeviceTester.compute_post_status(mask, status)
    np.testing.assert_array_equal(result, expected)

@pytest.mark.parametrize("mask,test_names,exp_names,exp_idx", [
    (np.array([[True, False, True], [True, True, True]]), ['a','b','c'], np.array(['b', None], dtype=object), np.array([1, -1])),
    (np.empty((2,0), dtype=bool), [], np.array([None,None], dtype=object), np.array([-1,-1])),
])
def test_compute_first_failure(mask, test_names, exp_names, exp_idx):
    names, idx = DeviceTester.compute_first_failure(mask, test_names)
    np.testing.assert_array_equal(names, exp_names)
    np.testing.assert_array_equal(idx, exp_idx)


def test_build_failure_context_vectorized(sample_csvs):
    dev_path, lim_path, devices, limits = sample_csvs
    tester = DeviceTester(dev_path, lim_path)
    tester.load_data()
    # create a mask where dev1 fails test_B only
    mask = np.array([[True, False], [True, True], [False, True]])
    names, idx = DeviceTester.compute_first_failure(mask, tester.test_names)
    ctx = DeviceTester.build_failure_context_vectorized(
        tester.df_values,
        names,
        idx,
        tester.limits
    )
    # Only include dev1 and dev3
    assert list(ctx['device_id']) == ['dev1', 'dev3']
    assert list(ctx['first_failure_test']) == ['test_B', 'test_A']
    # Check failure_value matches
    assert ctx.loc[ctx['device_id']=='dev1','failure_value'].iloc[0] == devices.loc['dev1','test_B']


def test_run_pipeline(sample_csvs, capsys):
    dev_path, lim_path, devices, limits = sample_csvs
    tester = DeviceTester(dev_path, lim_path)
    results = tester.run(timing=False)
    # Verify keys in results
    for key in ['df_results','first_fail_context','mask','post_status','first_failure_names','first_failure_indices']:
        assert key in results
    # post_status column added
    assert 'post_test_status' in results['df_results'].columns
    # first_failure_test column added
    assert 'first_failure_test' in results['df_results'].columns
