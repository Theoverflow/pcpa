from device_tester import DeviceTester

def main():
    tester = DeviceTester(
        devices_csv="devices.csv",
        limits_csv="test_limits.csv"
    )
    results = tester.run(timing=True)

    # If you want to export the recomputed CSVs:
    results["df_results"].to_csv("devices_recomputed.csv", index=True)
    results["first_fail_context"].to_csv("first_failure_context.csv", index=False)

if __name__ == "__main__":
    main()
