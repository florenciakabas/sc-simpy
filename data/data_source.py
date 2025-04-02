# data/data_source.py
from typing import Protocol, Dict, Any, List, Optional, Union
from abc import ABC, abstractmethod
import json
import os
import random
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


# We'll implement the Databricks data source later
class DatabricksDataSource(DataSource):
    """Data source that reads from Databricks tables using SQLAlchemy."""
    
    def __init__(self, connection_string: str):
        """
        Initialize with Databricks connection string.
        
        Args:
            connection_string: SQLAlchemy connection string for Databricks
        """
        self.connection_string = connection_string
        # We'll implement the actual connection logic later
    
    def get_ships_data(self) -> List[Dict[str, Any]]:
        """Retrieve ships configuration data."""
        # Placeholder - will implement later
        return []
    
    def get_customers_data(self) -> List[Dict[str, Any]]:
        """Retrieve customers configuration data."""
        # Placeholder - will implement later
        return []
    
    def get_distance_matrix(self) -> Dict[str, Dict[str, float]]:
        """Retrieve the distance matrix between locations."""
        # Placeholder - will implement later
        return {}
    
    def get_simulation_params(self) -> Dict[str, Any]:
        """Retrieve simulation parameters."""
        # Placeholder - will implement later
        return {}
    
    def save_results(self, results: Dict[str, Any]) -> None:
        """Save simulation results to Databricks."""
        # Placeholder - will implement later
        pass


def get_data_source(source_type: str, **kwargs) -> DataSource:
    """
    Factory function to create the appropriate data source.
    
    Args:
        source_type: Type of data source ("json" or "databricks")
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
        connection_string = kwargs.get("connection_string")
        if not connection_string:
            raise ValueError("connection_string is required for Databricks data source")
        return DatabricksDataSource(connection_string)
    else:
        raise ValueError(f"Unknown data source type: {source_type}")