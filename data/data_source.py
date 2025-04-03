# data/data_source.py
import ipdb
from typing import Protocol, Dict, Any, List, Optional, Union
from abc import ABC, abstractmethod
import json
import os
import pandas as pd
import random
from datetime import datetime
from sqlalchemy import create_engine

from typing import Any, Dict, List, Optional
from pathlib import Path
import yaml
import os
from datetime import datetime


class DataSource(ABC):
    """Abstract base class for data sources."""
    
    @abstractmethod
    def get_ships_data(self) -> List[Dict[str, Any]]:
        """Retrieve ships configuration data."""
        pass
    
    @abstractmethod
    def get_customers_data(self) -> List[Dict[str, Any]]:
        """Retrieve customers configuration data."""
        pass
    
    @abstractmethod
    def get_distance_matrix(self) -> Dict[str, Dict[str, float]]:
        """Retrieve the distance matrix between locations."""
        pass
    
    @abstractmethod
    def get_simulation_params(self) -> Dict[str, Any]:
        """Retrieve simulation parameters."""
        pass
    
    @abstractmethod
    def save_results(self, results: Dict[str, Any]) -> None:
        """Save simulation results."""
        pass

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

class JsonDataSource(DataSource):
    """Data source that reads from local JSON files."""
    
    def __init__(self, data_dir: str):
        """
        Initialize with directory containing JSON files.
        
        Args:
            data_dir: Directory containing JSON data files
        """
        self.data_dir = data_dir
        self._ensure_data_files_exist()
    
    def _ensure_data_files_exist(self) -> None:
        """Create example data files if they don't exist."""
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Check and generate ships data
        ships_file = os.path.join(self.data_dir, 'ships.json')
        if not os.path.exists(ships_file):
            self._generate_example_ships_data(ships_file)
        
        # Check and generate customers data
        customers_file = os.path.join(self.data_dir, 'customers.json')
        if not os.path.exists(customers_file):
            self._generate_example_customers_data(customers_file)
        
        # Check and generate distance matrix
        distances_file = os.path.join(self.data_dir, 'distances.json')
        if not os.path.exists(distances_file):
            self._generate_example_distance_matrix(distances_file)
        
        # Check and generate simulation parameters
        params_file = os.path.join(self.data_dir, 'simulation_params.json')
        if not os.path.exists(params_file):
            self._generate_example_simulation_params(params_file)
    
    def _generate_example_ships_data(self, file_path: str) -> None:
        """Generate example ships data and save to JSON."""
        ships = [
            {
                "id": "ship_1",
                "name": "Vessel Alpha",
                "capacity": 100000.0,  # 100,000 units capacity
                "speed": 25.0,         # 25 distance units per hour
                "initial_location": "port_main",
                "initial_cargo": 80000.0
            },
            {
                "id": "ship_2",
                "name": "Vessel Beta",
                "capacity": 75000.0,   # 75,000 units capacity
                "speed": 30.0,         # 30 distance units per hour
                "initial_location": "port_main",
                "initial_cargo": 60000.0
            },
            {
                "id": "ship_3",
                "name": "Vessel Gamma",
                "capacity": 120000.0,  # 120,000 units capacity
                "speed": 20.0,         # 20 distance units per hour
                "initial_location": "port_main",
                "initial_cargo": 100000.0
            }
        ]
        
        with open(file_path, 'w') as f:
            json.dump(ships, f, indent=2)
    
    def _generate_example_customers_data(self, file_path: str) -> None:
        """Generate example customers data and save to JSON."""
        customers = [
            {
                "id": "customer_1",
                "name": "Manufacturing Plant A",
                "location": "location_a",
                "demand_rate": 1000.0,      # 1,000 units per hour
                "initial_inventory": 48000.0,
                "min_inventory": 24000.0,   # 24,000 units (1 day)
                "max_inventory": 120000.0   # 120,000 units (5 days)
            },
            {
                "id": "customer_2",
                "name": "Distribution Center B",
                "location": "location_b",
                "demand_rate": 750.0,       # 750 units per hour
                "initial_inventory": 36000.0,
                "min_inventory": 18000.0,   # 18,000 units (1 day)
                "max_inventory": 90000.0    # 90,000 units (5 days)
            },
            {
                "id": "customer_3",
                "name": "Processing Facility C",
                "location": "location_c",
                "demand_rate": 1200.0,      # 1,200 units per hour
                "initial_inventory": 57600.0,
                "min_inventory": 28800.0,   # 28,800 units (1 day)
                "max_inventory": 144000.0   # 144,000 units (5 days)
            }
        ]
        
        with open(file_path, 'w') as f:
            json.dump(customers, f, indent=2)
    
    def _generate_example_distance_matrix(self, file_path: str) -> None:
        """Generate example distance matrix and save to JSON."""
        # Locations: port_main, location_a, location_b, location_c
        locations = ["port_main", "location_a", "location_b", "location_c"]
        
        # Create a distance matrix with realistic values
        matrix = {}
        for from_loc in locations:
            matrix[from_loc] = {}
            for to_loc in locations:
                if from_loc == to_loc:
                    # Zero distance to self
                    matrix[from_loc][to_loc] = 0.0
                else:
                    # Random distance between 200 and 800 (nautical miles)
                    # But ensure symmetry in the matrix
                    if to_loc in matrix and from_loc in matrix[to_loc]:
                        matrix[from_loc][to_loc] = matrix[to_loc][from_loc]
                    else:
                        matrix[from_loc][to_loc] = random.uniform(200.0, 800.0)
        
        with open(file_path, 'w') as f:
            json.dump(matrix, f, indent=2)
    
    def _generate_example_simulation_params(self, file_path: str) -> None:
        """Generate example simulation parameters and save to JSON."""
        params = {
            "simulation_duration": 720.0,   # 30 days (in hours)
            "time_step": 1.0,               # 1 hour time steps
            "resupply_threshold_days": 3.0, # Trigger resupply when inventory falls below 3 days
            "loading_rate": 5000.0,         # Units per hour for loading
            "unloading_rate": 4000.0,       # Units per hour for unloading
            "port_resupply_delay": 12.0,    # Hours delay for resupply at port
            "random_seed": 42               # Random seed for reproducibility
        }
        
        with open(file_path, 'w') as f:
            json.dump(params, f, indent=2)
    
    def get_ships_data(self) -> List[Dict[str, Any]]:
        """Retrieve ships configuration data."""
        file_path = os.path.join(self.data_dir, 'ships.json')
        with open(file_path, 'r') as f:
            return json.load(f)
    
    def get_customers_data(self) -> List[Dict[str, Any]]:
        """Retrieve customers configuration data."""
        file_path = os.path.join(self.data_dir, 'customers.json')
        with open(file_path, 'r') as f:
            return json.load(f)
    
    def get_distance_matrix(self) -> Dict[str, Dict[str, float]]:
        """Retrieve the distance matrix between locations."""
        file_path = os.path.join(self.data_dir, 'distances.json')
        with open(file_path, 'r') as f:
            return json.load(f)
    
    def get_simulation_params(self) -> Dict[str, Any]:
        """Retrieve simulation parameters."""
        file_path = os.path.join(self.data_dir, 'simulation_params.json')
        with open(file_path, 'r') as f:
            return json.load(f)
    
    def save_results(self, results: Dict[str, Any]) -> None:
        """Save simulation results to a JSON file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(self.data_dir, f'results_{timestamp}.json')
        with open(file_path, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {file_path}")


# Add this to the get_data_source function in data/data_source.py
def get_data_source(source_type: str, **kwargs) -> DataSource:
    """
    Factory function to create the appropriate data source.
    
    Args:
        source_type: Type of data source ("json", "databricks")
        **kwargs: Additional arguments specific to the data source type
    
    Returns:
        Configured data source instance
    
    Raises:
        ValueError: If source_type is not recognized
    """
    if source_type.lower() == "json":
        data_dir = kwargs.get("data_dir", "./data")
        return JsonDataSource(data_dir)
    elif source_type.lower() == "databricks":
        # Check for required parameters
        host = kwargs.get("host")
        http_path = kwargs.get("http_path")
        token = kwargs.get("token")
        
        if not all([host, http_path, token]):
            raise ValueError("host, http_path, and token are required for Databricks data source")
        
        # Optional parameters with defaults
        catalog = kwargs.get("catalog", "hive_metastore")
        schema = kwargs.get("schema", "default")
        ships_table = kwargs.get("ships_table", "ships")
        customers_table = kwargs.get("customers_table", "customers")
        distances_table = kwargs.get("distances_table", "distances")
        params_table = kwargs.get("params_table", "simulation_params")
        results_table = kwargs.get("results_table", "simulation_results")
        
        return DatabricksDataSource(
            host=host,
            http_path=http_path,
            token=token,
            catalog=catalog,
            schema=schema,
            ships_table=ships_table,
            customers_table=customers_table,
            distances_table=distances_table,
            params_table=params_table,
            results_table=results_table
        )
    else:
        raise ValueError(f"Unknown data source type: {source_type}")