# run_scenario.py
from simulation.scenario_manager import ScenarioManager

def main():
    """Example of using Kedro for scenario management."""
    print("Supply Chain Simulation Scenario Management")
    print("==========================================\n")
    
    # Create the scenario manager
    manager = ScenarioManager()
    
    # Create baseline scenario
    baseline = manager.create_scenario(
        name="baseline",
        description="Baseline configuration with default parameters",
        param_overrides=None
    )
    
    # Run baseline scenario
    manager.run_scenario(baseline)
    
    # Create and run variations
    scenarios = [
        {
            "name": "fast_loading",
            "description": "Increased loading rate scenario",
            "params": {"loading_rate": 8000.0}
        },
        {
            "name": "high_threshold",
            "description": "Higher inventory threshold for resupply",
            "params": {"resupply_threshold_days": 5.0}
        },
        {
            "name": "faster_ships",
            "description": "Ships with 20% higher speed",
            "params": {"ship_speed_multiplier": 1.2}
        }
    ]
    
    # Create and run each scenario
    for scenario in scenarios:
        name = manager.create_scenario(
            name=scenario["name"],
            description=scenario["description"],
            param_overrides=scenario["params"],
            base_scenario="baseline"
        )
        manager.run_scenario(name)
    
    # Compare all scenarios
    scenario_names = ["baseline"] + [s["name"] for s in scenarios]
    comparison = manager.compare_scenarios(scenario_names)
    
    # Print comparison results
    print("\nScenario Comparison:")
    print("===================")
    for name, metrics in comparison["scenarios"].items():
        print(f"\n{name}:")
        print(f"  Service Level: {metrics.get('overall_service_level', 0):.2%}")
        print(f"  Stockout Events: {metrics.get('total_stockout_events', 0)}")
        if metrics.get("param_overrides"):
            print("  Parameter Overrides:")
            for param, value in metrics["param_overrides"].items():
                print(f"    {param}: {value}")

if __name__ == "__main__":
    main()