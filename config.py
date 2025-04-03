# config.py
"""Configuration settings for the supply chain simulation."""

# Databricks connection settings
DATABRICKS_CONFIG = {
    # Fill these with your actual Databricks credentials
    "host": "adb-your-workspace-id.azuredatabricks.net",  # Replace with your workspace host
    "http_path": "/sql/1.0/warehouses/your-warehouse-id",  # Replace with your HTTP path
    "token": "your-databricks-token",  # Replace with your token
    "catalog": "hive_metastore",  # Default catalog
    "schema": "default",  # Default schema
}

# Default data source type ("json" or "databricks")
DEFAULT_DATA_SOURCE = "json"

# Data directory for JSON data source
JSON_DATA_DIR = "./data_files"

# Kedro configuration
KEDRO_CONFIG = {
    "catalog_config": "catalog.yml",
    "default_scenario": "baseline"
}

# Update DEFAULT_DATA_SOURCE to use Kedro if desired
DEFAULT_DATA_SOURCE = "kedro"  # Options: "json", "databricks", "kedro"