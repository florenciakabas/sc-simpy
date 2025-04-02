# Example in a separate script (run_databricks.py)
from config import DATABRICKS_CONFIG
from main import run_with_databricks

# Run simulation with Databricks data
results = run_with_databricks(
    host=DATABRICKS_CONFIG["host"],
    http_path=DATABRICKS_CONFIG["http_path"],
    token=DATABRICKS_CONFIG["token"],
    catalog=DATABRICKS_CONFIG["catalog"],
    schema=DATABRICKS_CONFIG["schema"]
)