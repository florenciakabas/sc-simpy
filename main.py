# Example usage in main.py
from data.data_source import get_data_source

def main():
    # For local development with JSON files
    data_source = get_data_source("json", data_dir="./data_files")
    
    # Load configuration
    ships_data = data_source.get_ships_data()
    customers_data = data_source.get_customers_data()
    distance_matrix = data_source.get_distance_matrix()
    simulation_params = data_source.get_simulation_params()
    
    # Later we'll use this data to initialize our simulation
    print(f"Loaded {len(ships_data)} ships and {len(customers_data)} customers")
    print(f"Simulation will run for {simulation_params['simulation_duration']} hours")

if __name__ == "__main__":
    main()