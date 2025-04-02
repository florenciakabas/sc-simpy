# data/databricks_client.py
from typing import Dict, List, Any, Optional
import json
from datetime import datetime

from sqlalchemy import create_engine, text
import pandas as pd

from data.data_source import DataSource


class DatabricksDataSource(DataSource):
    """Data source that reads from Databricks tables using SQLAlchemy."""
    
    def __init__(
        self, 
        host: str,
        http_path: str,
        token: str,
        catalog: str = "hive_metastore",
        schema: str = "default",
        ships_table: str = "ships",
        customers_table: str = "customers",
        distances_table: str = "distances",
        params_table: str = "simulation_params",
        results_table: str = "simulation_results"
    ):
        """
        Initialize with Databricks connection parameters.
        
        Args:
            host: Databricks workspace hostname (e.g., adb-123456789.1.azuredatabricks.net)
            http_path: HTTP path (e.g., /sql/1.0/warehouses/abcdef123456)
            token: Databricks personal access token
            catalog: Databricks catalog (default: hive_metastore)
            schema: Database schema (default: default)
            ships_table: Table name for ships data
            customers_table: Table name for customers data
            distances_table: Table name for distances data
            params_table: Table name for simulation parameters
            results_table: Table name for simulation results
        """
        self.host = host
        self.http_path = http_path
        self.token = token
        self.catalog = catalog
        self.schema = schema
        self.ships_table = ships_table
        self.customers_table = customers_table
        self.distances_table = distances_table
        self.params_table = params_table
        self.results_table = results_table
        
        # Build connection string
        conn_str = f"databricks://token:{token}@{host}?http_path={http_path}&catalog={catalog}&schema={schema}"
        self.engine = create_engine(conn_str)
    
    def get_ships_data(self) -> List[Dict[str, Any]]:
        """Retrieve ships configuration data from Databricks."""
        query = f"SELECT * FROM {self.ships_table}"
        
        try:
            df = pd.read_sql(query, self.engine)
            # Convert DataFrame to list of dictionaries
            return df.to_dict(orient="records")
        except Exception as e:
            print(f"Error retrieving ships data: {e}")
            # Return empty list in case of error
            return []
    
    def get_customers_data(self) -> List[Dict[str, Any]]:
        """Retrieve customers configuration data from Databricks."""
        query = f"SELECT * FROM {self.customers_table}"
        
        try:
            df = pd.read_sql(query, self.engine)
            return df.to_dict(orient="records")
        except Exception as e:
            print(f"Error retrieving customers data: {e}")
            return []
    
    def get_distance_matrix(self) -> Dict[str, Dict[str, float]]:
        """Retrieve the distance matrix between locations from Databricks."""
        query = f"SELECT from_location, to_location, distance FROM {self.distances_table}"
        
        try:
            df = pd.read_sql(query, self.engine)
            
            # Convert from flat table to nested dictionary format
            matrix = {}
            for _, row in df.iterrows():
                from_loc = row["from_location"]
                to_loc = row["to_location"]
                distance = float(row["distance"])
                
                if from_loc not in matrix:
                    matrix[from_loc] = {}
                
                matrix[from_loc][to_loc] = distance
            
            return matrix
        except Exception as e:
            print(f"Error retrieving distance matrix: {e}")
            return {}
    
    def get_simulation_params(self) -> Dict[str, Any]:
        """Retrieve simulation parameters from Databricks."""
        query = f"SELECT param_name, param_value, param_type FROM {self.params_table}"
        
        try:
            df = pd.read_sql(query, self.engine)
            
            # Convert from table format to dictionary, with proper type conversion
            params = {}
            for _, row in df.iterrows():
                name = row["param_name"]
                value = row["param_value"]
                param_type = row["param_type"].lower()
                
                # Convert value based on parameter type
                if param_type == "float":
                    params[name] = float(value)
                elif param_type == "int":
                    params[name] = int(value)
                elif param_type == "bool":
                    params[name] = value.lower() in ("true", "yes", "1")
                else:
                    # Default: keep as string
                    params[name] = value
            
            return params
        except Exception as e:
            print(f"Error retrieving simulation parameters: {e}")
            return {}
    
    def save_results(self, results: Dict[str, Any]) -> None:
        """
        Save simulation results to Databricks.
        
        This serializes the results dict to JSON and saves it to the results table.
        """
        # Convert results to JSON string
        results_json = json.dumps(results)
        
        # Create timestamp
        timestamp = datetime.now().isoformat()
        
        # Create a DataFrame with the results
        df = pd.DataFrame([{
            "run_id": f"run_{timestamp}",
            "timestamp": timestamp,
            "overall_service_level": results.get("metrics", {}).get("overall_service_level", 0.0),
            "results_json": results_json
        }])
        
        try:
            # Write results to Databricks table
            df.to_sql(
                self.results_table, 
                self.engine, 
                if_exists="append", 
                index=False
            )
            print(f"Results saved to Databricks table {self.schema}.{self.results_table}")
        except Exception as e:
            print(f"Error saving results to Databricks: {e}")
            # Fallback: save to local file
            local_file = f"./results_{timestamp.replace(':', '-')}.json"
            with open(local_file, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"Failed to save to Databricks. Results saved locally to {local_file}")