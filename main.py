# main.py
import os
import matplotlib.pyplot as plt
import pandas as pd
from typing import List, Dict, Any

from data.data_source import get_data_source
from simulation.engine import SupplyChainSimulation, ParameterStudy


def run_single_simulation(data_source_type="json", **kwargs):
    """Run a single simulation."""
    # Get data source
    data_source = get_data_source(data_source_type, **kwargs)
    
    # Create and run simulation
    simulation = SupplyChainSimulation(data_source)
    results = simulation.run()
    
    print("\nSimulation completed. Key metrics:")
    print(f"Overall service level: {results['metrics'].get('overall_service_level', 'N/A'):.2%}")
    
    # Print customer metrics
    print("\nCustomer metrics:")
    for customer_id, metrics in results["metrics"].get("customer_metrics", {}).items():
        print(f"  {customer_id}: Service level: {metrics['service_level']:.2%}, " +
              f"Avg inventory: {metrics['avg_inventory']:.1f}, " +
              f"Stockout hours: {metrics['stockout_hours']:.1f}")
    
    # Print ship metrics
    print("\nShip metrics:")
    for ship_id, metrics in results["metrics"].get("ship_metrics", {}).items():
        print(f"  {ship_id}: Deliveries: {metrics['num_deliveries']}, " +
              f"Resupplies: {metrics['num_resupplies']}, " +
              f"Total distance: {metrics['total_distance']:.1f}")
    
    return results


def run_parameter_study(
    param_name: str, 
    param_values: List[Any],
    data_source_type="json", 
    **kwargs
):
    """Run a parameter study."""
    # Get data source
    data_source = get_data_source(data_source_type, **kwargs)
    
    # Create and run parameter study
    study = ParameterStudy(data_source, param_name, param_values)
    results = study.run()
    
    # Plot results
    plot_parameter_study(results, param_name)
    
    return results


def plot_parameter_study(results: List[Dict[str, Any]], param_name: str):
    """Plot the results of a parameter study."""
    # Extract key metrics
    x_values = [result["param_value"] for result in results]
    service_levels = [result["metrics"].get("overall_service_level", 0) for result in results]
    
    # Plot service level vs parameter value
    plt.figure(figsize=(10, 6))
    plt.plot(x_values, service_levels, marker='o', linestyle='-')
    plt.xlabel(param_name)
    plt.ylabel("Overall Service Level")
    plt.title(f"Effect of {param_name} on Service Level")
    plt.grid(True)
    
    # Save plot
    os.makedirs("./outputs", exist_ok=True)
    plt.savefig(f"./outputs/parameter_study_{param_name}.png")
    plt.close()
    
    print(f"Parameter study plot saved to ./outputs/parameter_study_{param_name}.png")

    
def plot_inventory_levels(simulation_results: Dict[str, Any]):
    """Plot inventory levels over time for each customer."""
    if not simulation_results.get("customers_history"):
        print("No customer history data available for plotting")
        return
    
    # Create DataFrame from customer history
    df = pd.DataFrame(simulation_results["customers_history"])
    
    # Plot inventory over time for each customer
    plt.figure(figsize=(12, 8))
    
    for customer_id in df["customer_id"].unique():
        customer_data = df[df["customer_id"] == customer_id]
        customer_name = customer_data["customer_name"].iloc[0]
        
        plt.plot(
            customer_data["time"], 
            customer_data["inventory"],
            label=f"{customer_name} ({customer_id})"
        )
    
    plt.xlabel("Time (hours)")
    plt.ylabel("Inventory Level")
    plt.title("Customer Inventory Levels Over Time")
    plt.legend()
    plt.grid(True)
    
    # Save plot
    os.makedirs("./outputs", exist_ok=True)
    plt.savefig("./outputs/inventory_levels.png")
    plt.close()
    
    print("Inventory levels plot saved to ./outputs/inventory_levels.png")

def run_with_databricks(
    host, 
    http_path, 
    token, 
    catalog="hive_metastore", 
    schema="default"
):
    """Run the simulation with Databricks data source."""
    print("Running simulation with Databricks data source...")
    
    # Create Databricks data source
    results = run_single_simulation(
        data_source_type="databricks",
        host=host,
        http_path=http_path,
        token=token,
        catalog=catalog,
        schema=schema
    )
    
    # Plot inventory levels
    plot_inventory_levels(results)
    
    print("\nSimulation completed!")
    
    return results

def run_with_fallback():
    """
    Try to run with Databricks, fall back to JSON if it fails.
    """
    from config import DATABRICKS_CONFIG, DEFAULT_DATA_SOURCE, JSON_DATA_DIR
    
    if DEFAULT_DATA_SOURCE.lower() == "databricks":
        try:
            return run_with_databricks(
                host=DATABRICKS_CONFIG["host"],
                http_path=DATABRICKS_CONFIG["http_path"],
                token=DATABRICKS_CONFIG["token"],
                catalog=DATABRICKS_CONFIG["catalog"],
                schema=DATABRICKS_CONFIG["schema"]
            )
        except Exception as e:
            print(f"Databricks connection failed: {e}")
            print("Falling back to JSON data source...")
    
    # Fall back to JSON
    return run_single_simulation(
        data_source_type="json",
        data_dir=JSON_DATA_DIR
    )

def main():
    """Main entry point for the application."""
    print("Supply Chain Simulation")
    print("======================\n")
    
    # Create data directory if it doesn't exist
    os.makedirs("./data_files", exist_ok=True)
    
    # 1. Run a single simulation
    print("Running single simulation...")
    results = run_single_simulation(data_source_type="json", data_dir="./data_files")
    
    # 2. Plot inventory levels
    plot_inventory_levels(results)
    
    # 3. Run a parameter study
    print("\nRunning parameter study on resupply threshold...")
    param_values = [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
    run_parameter_study(
        param_name="resupply_threshold_days",
        param_values=param_values,
        data_source_type="json",
        data_dir="./data_files"
    )
    
    print("\nSimulation study completed!")


if __name__ == "__main__":
    main()