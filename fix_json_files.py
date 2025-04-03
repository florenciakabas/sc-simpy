# fix_json_files.py
import json
import os
from pathlib import Path

def fix_json_file(file_path, default_content):
    """Fix a corrupted JSON file or create it if it doesn't exist."""
    path = Path(file_path)
    
    # Create directory if it doesn't exist
    os.makedirs(path.parent, exist_ok=True)
    
    # Check if file exists and is valid JSON
    if path.exists():
        try:
            with open(path, 'r') as f:
                # Try to load the JSON
                json.load(f)
                print(f"✅ {path.name} is valid JSON")
                return False  # No repair needed
        except json.JSONDecodeError:
            print(f"❌ {path.name} is corrupted - repairing...")
    else:
        print(f"❓ {path.name} doesn't exist - creating...")
    
    # Write the default content
    with open(path, 'w') as f:
        json.dump(default_content, f, indent=2)
        
    print(f"✅ {path.name} has been fixed/created")
    return True  # Repair performed

def main():
    """Check and fix all required JSON files."""
    data_dir = Path("./data_files")
    os.makedirs(data_dir, exist_ok=True)
    
    # Default ship data
    ships_data = [
        {
            "id": "ship_1",
            "name": "Vessel Alpha",
            "capacity": 100000.0,
            "speed": 25.0,
            "initial_location": "port_main",
            "initial_cargo": 80000.0
        },
        {
            "id": "ship_2",
            "name": "Vessel Beta",
            "capacity": 75000.0,
            "speed": 30.0,
            "initial_location": "port_main",
            "initial_cargo": 60000.0
        },
        {
            "id": "ship_3",
            "name": "Vessel Gamma",
            "capacity": 120000.0,
            "speed": 20.0,
            "initial_location": "port_main",
            "initial_cargo": 100000.0
        }
    ]
    
    # Default customer data
    customers_data = [
        {
            "id": "customer_1",
            "name": "Manufacturing Plant A",
            "location": "location_a",
            "demand_rate": 1000.0,
            "initial_inventory": 48000.0,
            "min_inventory": 24000.0,
            "max_inventory": 120000.0
        },
        {
            "id": "customer_2",
            "name": "Distribution Center B",
            "location": "location_b",
            "demand_rate": 750.0,
            "initial_inventory": 36000.0,
            "min_inventory": 18000.0,
            "max_inventory": 90000.0
        },
        {
            "id": "customer_3",
            "name": "Processing Facility C",
            "location": "location_c",
            "demand_rate": 1200.0,
            "initial_inventory": 57600.0,
            "min_inventory": 28800.0,
            "max_inventory": 144000.0
        }
    ]
    
    # Default distance matrix
    distances_data = {
        "port_main": {
            "port_main": 0.0,
            "location_a": 450.0,
            "location_b": 600.0,
            "location_c": 750.0
        },
        "location_a": {
            "port_main": 450.0,
            "location_a": 0.0,
            "location_b": 250.0,
            "location_c": 400.0
        },
        "location_b": {
            "port_main": 600.0,
            "location_a": 250.0,
            "location_b": 0.0,
            "location_c": 300.0
        },
        "location_c": {
            "port_main": 750.0,
            "location_a": 400.0,
            "location_b": 300.0,
            "location_c": 0.0
        }
    }
    
    # Default simulation parameters
    params_data = {
        "simulation_duration": 720.0,
        "time_step": 1.0,
        "resupply_threshold_days": 3.0,
        "loading_rate": 5000.0,
        "unloading_rate": 4000.0,
        "port_resupply_delay": 12.0,
        "random_seed": 42
    }
    
    # Fix all required files
    fix_json_file(data_dir / "ships.json", ships_data)
    fix_json_file(data_dir / "customers.json", customers_data)
    fix_json_file(data_dir / "distances.json", distances_data)
    fix_json_file(data_dir / "simulation_params.json", params_data)
    
    print("\nAll JSON files have been checked and fixed if necessary.")

if __name__ == "__main__":
    main()