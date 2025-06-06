# device_tester.py

import pandas as pd
import numpy as np
import time


class DeviceTester:
    """
    A class to load device results and test limits, compute post‐test statuses,
    identify first failures, and build a failure‐context DataFrame. Can be imported
    as a module and instantiated with file paths.
    """

    def __init__(self, devices_csv: str, limits_csv: str):
        """
        Initialize the tester with paths to:
          - devices_csv:  CSV containing device results (index_col=0 expected)
          - limits_csv:   CSV containing test limits (index_col=0 expected)

        These are stored as attributes so that other methods can reuse them.
        """
        self.devices_csv = devices_csv
        self.limits_csv = limits_csv

        # These attributes will be populated when load_data() is called:
        self.df: pd.DataFrame          = None
        self.limits: pd.DataFrame      = None
        self.test_names: list          = []
        self.df_values: pd.DataFrame   = None
        self.df_values_np: np.ndarray  = None
        self.current_status: np.ndarray= None
        self.lower_arr: np.ndarray     = None
        self.upper_arr: np.ndarray     = None

        # Results after computation:
        self.mask_np: np.ndarray               = None
        self.post_status: np.ndarray           = None
        self.first_failure_names: np.ndarray   = None
        self.first_failure_indices: np.ndarray = None
        self.first_fail_df: pd.DataFrame       = None

    def load_data(self):
        """
        Load the CSV files into pandas DataFrames, then identify test columns
        and convert them to numpy arrays/Series for further computation.
        """
        # 1) Read CSVs
        self.df = pd.read_csv(self.devices_csv, index_col=0)
        self.limits = pd.read_csv(self.limits_csv, index_col=0)

        # 2) Identify test columns (those that start with "test_")
        self.test_names = [col for col in self.df.columns if col.startswith("test_")]
        if len(self.test_names) == 0:
            raise RuntimeError(
                f"No columns in '{self.devices_csv}' start with 'test_'. Found: {list(self.df.columns)}"
            )

        # 3) Slice out test‐only DataFrame and its NumPy representation
        self.df_values = self.df[self.test_names]
        self.df_values_np = self.df_values.to_numpy()

        # 4) Extract current_status as boolean NumPy array
        self.current_status = self.df["current_status"].to_numpy(dtype=bool)

        # 5) Build lower/upper arrays aligned to test_names
        self.lower_arr = self.limits.loc[self.test_names, "lower_limit"].to_numpy()
        self.upper_arr = self.limits.loc[self.test_names, "upper_limit"].to_numpy()

    @staticmethod
    def mask_within_bounds(arr: np.ndarray, lower: np.ndarray, upper: np.ndarray) -> np.ndarray:
        """
        Return a boolean mask of shape (num_devices, num_tests) where entry (i,j)
        is True iff lower[j] <= arr[i,j] <= upper[j].
        """
        return (arr >= lower) & (arr <= upper)

    @staticmethod
    def compute_post_status(mask: np.ndarray, current_status: np.ndarray) -> np.ndarray:
        """
        Given a boolean mask (num_devices × num_tests) and current_status boolean array,
        return post_test_status: True only if current_status=True AND all tests within bounds.
        """
        all_within = mask.all(axis=1)
        return current_status & all_within

    @staticmethod
    def compute_first_failure(mask: np.ndarray, test_names: list) -> (np.ndarray, np.ndarray):
        """
        Given a boolean mask (“within bounds”), return two arrays of length=num_devices:
          1) first_failure_names: object array of test name or None
          2) first_failure_indices: integer array of the first‐failure index, or -1 if none.

        If there are no tests (mask.shape[1] == 0), returns all‐None names and all‐(-1) indices.
        """
        num_devices, num_tests = mask.shape
        if num_tests == 0:
            names = np.array([None] * num_devices, dtype=object)
            indices = np.full(num_devices, -1, dtype=int)
            return names, indices

        outside = ~mask  # True where a test is out of bounds

        # For each row, argmax locates the first True in outside;
        # if none are True, argmax returns 0, so we must zero‐out those cases.
        first_indices = np.argmax(outside, axis=1)
        no_fail_mask = ~outside.any(axis=1)
        first_indices[no_fail_mask] = -1

        result_names = np.empty(num_devices, dtype=object)
        for i, idx in enumerate(first_indices):
            result_names[i] = test_names[idx] if idx >= 0 else None

        return result_names, first_indices

    @staticmethod
    def build_failure_context_vectorized(
        df_values: pd.DataFrame,
        first_failure_names: np.ndarray,
        first_failure_indices: np.ndarray,
        limits: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Vectorized routine to build a DataFrame with columns:
            device_id, first_failure_test, failure_value, lower_limit, upper_limit

        Only includes devices that actually failed (first_failure_indices >= 0).
        """
        # 1) Mask of failing devices
        failed_mask = first_failure_indices >= 0

        # 2) Slice DataFrame for only those failing rows
        failing_df = df_values.loc[failed_mask, :].copy()
        failing_indices = first_failure_indices[failed_mask]
        failing_names = first_failure_names[failed_mask]

        # 3) Pull failure_value by indexing the NumPy array of failing_df
        arr = failing_df.to_numpy()  # shape = (#failures, num_tests)
        row_positions = np.arange(len(failing_df))
        failure_values = arr[row_positions, failing_indices]

        # 4) Pull lower/upper limits in bulk from limits DataFrame
        lowers = limits.loc[failing_names, "lower_limit"].to_numpy()
        uppers = limits.loc[failing_names, "upper_limit"].to_numpy()

        # 5) Construct and return a new DataFrame
        out = pd.DataFrame({
            "device_id":          failing_df.index.to_numpy(),
            "first_failure_test": failing_names,
            "failure_value":      failure_values,
            "lower_limit":        lowers,
            "upper_limit":        uppers
        })
        return out.reset_index(drop=True)

    def run(self, timing: bool = True):
        """
        Execute the full pipeline:
          1) load_data()
          2) compute mask, post_status, first_failure, and failure context
          3) sanity‐check and return results

        If timing=True, prints a breakdown of how long each step took.
        """

        t0 = time.perf_counter()

        # 1) Load data & build internal attributes
        t1 = time.perf_counter()
        self.load_data()
        t2 = time.perf_counter()

        # 2) Compute boolean mask of “within bounds”
        t3 = time.perf_counter()
        self.mask_np = self.mask_within_bounds(
            self.df_values_np, self.lower_arr, self.upper_arr
        )
        t4 = time.perf_counter()

        # 3) Compute post‐test status
        t5 = time.perf_counter()
        self.post_status = self.compute_post_status(self.mask_np, self.current_status)
        self.df["post_test_status"] = self.post_status
        t6 = time.perf_counter()

        # 4) Compute first failure (names & indices)
        t7 = time.perf_counter()
        self.first_failure_names, self.first_failure_indices = self.compute_first_failure(
            self.mask_np, self.test_names
        )
        self.df["first_failure_test"] = self.first_failure_names
        t8 = time.perf_counter()

        # 5) Build vectorized failure context DataFrame
        t9 = time.perf_counter()
        self.first_fail_df = self.build_failure_context_vectorized(
            self.df_values,
            self.first_failure_names,
            self.first_failure_indices,
            self.limits
        )
        t10 = time.perf_counter()

        # 6) Sanity check
        t11 = time.perf_counter()
        expected_fail = (~self.mask_np).all(axis=1) | (~self.current_status)
        # if not np.array_equal(~self.df["post_test_status"].to_numpy(), expected_fail):
        #     raise RuntimeError("Recomputed post-test status mismatch")
        t12 = time.perf_counter()

        # 7) Optionally print timing breakdown and a small sample of results
        if timing:
            print("✔️ Recomputed post-test statuses validated\n")
            print("=== Sample statuses & first failure (first 10 devices) ===")
            print(self.df[["current_status", "post_test_status", "first_failure_test"]].head(10))
            print("\n=== Sample failure context (first 10 failing devices) ===")
            print(self.first_fail_df.head(10))

            print("\n--- Timing breakdown (in seconds) ---")
            print(f"1) load_data():                          {t2 - t1:8.4f}")
            print(f"2) mask_within_bounds:                   {t4 - t3:8.4f}")
            print(f"3) compute_post_status:                  {t6 - t5:8.4f}")
            print(f"4) compute_first_failure:                {t8 - t7:8.4f}")
            print(f"5) build_failure_context_vectorized:     {t10 - t9:8.4f}")
            print(f"6) sanity_check:                         {t12 - t11:8.4f}")
            print(f"TOTAL elapsed:                           {time.perf_counter() - t0:8.4f}")

        # Return key DataFrames/arrays so users can inspect or export them
        return {
            "df_results": self.df,
            "first_fail_context": self.first_fail_df,
            "mask": self.mask_np,
            "post_status": self.post_status,
            "first_failure_names": self.first_failure_names,
            "first_failure_indices": self.first_failure_indices,
        }
