import pandas as pd
import numpy as np

# Define recipes and versions
recipes = ["REC-001", "REC-002", "REC-003"]
versions = ["v1.0.0", "v2.0.0", "v3.0.0"]

testnum=101

# Generate files
file_paths = []
for recipe in recipes:
    for version in versions:
        data = []
        # Generate 30 unique test parameters
        for i, test_number in enumerate(range(testnum, testnum + 30), start=1):
            target_value = round(1 + np.random.rand(), 3)  # Random target between 1.000 and 2.000
            tol = round(0.05 + 0.01 * np.random.rand(), 3)   # Random tolerance between 0.05 and 0.06
            data.append({
                "recipe_id": recipe,
                "version": version,
                "test_number": test_number,
                "test_name": f"Test_{test_number}",
                "characteristic": f"Characteristic_{test_number}",
                "target_value": target_value,
                "tol_lower": round(target_value - tol, 3),
                "tol_upper": round(target_value + tol, 3),
                "unit": "mm",
                "method_reference": f"STD-METH-{test_number}",
                "remarks": ""
            })
        df = pd.DataFrame(data)
        # Save CSV
        filename = f"CSV/{recipe}_{version}.csv"
        df.to_csv(filename, index=False)
        file_paths.append(filename)

        testnum += 100
    testnum += 100