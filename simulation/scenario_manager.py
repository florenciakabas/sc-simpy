# simulation/scenario_manager.py
from typing import Dict, List, Any, Optional
import json
import os
from datetime import datetime
from pathlib import Path
import shutil

from data.data_source import get_data_source
from simulation.engine import SupplyChainSimulation

class ScenarioManager:
    """
    Lightweight utility for managing scenarios in the supply chain simulation.
    """
    
    def __init__(self, data_dir: str = "./data_files"):
        """
        Initialize the scenario manager.
        
        Args:
            data_dir: Directory containing data files
        """
        self.data_dir = Path(data_dir)
        self.scenarios_dir = self.data_dir / "scenarios"
        self.metadata_dir = self.data_dir / "metadata"
        
        # Create directories if they don't exist
        os.makedirs(self.scenarios_dir, exist_ok=True)
        os.makedirs(self.metadata_dir, exist_ok=True)
        
        # Validate data files
        self._validate_data_files()
    
    def _validate_data_files(self):
        """Validate that required data files exist and are valid JSON."""
        required_files = ["ships.json", "customers.json", "distances.json", "simulation_params.json"]
        
        for file_name in required_files:
            file_path = self.data_dir / file_name
            if not file_path.exists():
                print(f"Warning: {file_name} does not exist")
                continue
            
            # Check if the file is valid JSON
            try:
                with open(file_path, 'r') as f:
                    json.load(f)
            except json.JSONDecodeError:
                print(f"Warning: {file_name} is not valid JSON")
                # Create a backup of the invalid file
                backup_path = file_path.with_suffix(f".bak.{datetime.now().strftime('%Y%m%d%H%M%S')}")
                shutil.copy2(file_path, backup_path)
                print(f"Created backup of invalid file at {backup_path}")
    
    def create_scenario(
        self, 
        name: str,
        description: str,
        param_overrides: Optional[Dict[str, Any]] = None,
        base_scenario: Optional[str] = None
    ) -> str:
        """
        Create a new scenario by modifying parameters.
        
        Args:
            name: Name for the new scenario
            description: Description of the scenario
            param_overrides: Parameter values to override
            base_scenario: Name of the scenario to use as a base (or None for latest)
        
        Returns:
            Name of the created scenario
        """
        # Get base data
        if base_scenario:
            # Load parameters from base scenario
            base_params_file = self.scenarios_dir / f"{base_scenario}_params.json"
            if base_params_file.exists():
                try:
                    with open(base_params_file, 'r') as f:
                        params = json.load(f)
                except json.JSONDecodeError:
                    print(f"Warning: {base_scenario}_params.json is not valid JSON")
                    # Fall back to default params
                    data_source = get_data_source("json", data_dir=str(self.data_dir))
                    params = data_source.get_simulation_params()
            else:
                # Fall back to default params
                data_source = get_data_source("json", data_dir=str(self.data_dir))
                params = data_source.get_simulation_params()
        else:
            # Use default parameters
            data_source = get_data_source("json", data_dir=str(self.data_dir))
            params = data_source.get_simulation_params()
        
        # Apply overrides
        if param_overrides:
            for key, value in param_overrides.items():
                params[key] = value
        
        # Save params for this scenario
        scenario_params_file = self.scenarios_dir / f"{name}_params.json"
        with open(scenario_params_file, 'w') as f:
            json.dump(params, f, indent=2)
        
        # Save metadata
        metadata = {
            "name": name,
            "description": description,
            "base_scenario": base_scenario,
            "param_overrides": param_overrides,
            "created_at": datetime.now().isoformat()
        }
        
        metadata_file = self.metadata_dir / f"{name}.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"Created scenario '{name}' based on '{base_scenario or 'default'}'")
        return name
    
    def run_scenario(self, name: str) -> Dict[str, Any]:
        """
        Run a specific named scenario.
        
        Args:
            name: Name of the scenario to run
        
        Returns:
            Simulation results
        """
        # Load parameters for this scenario
        scenario_params_file = self.scenarios_dir / f"{name}_params.json"
        if not scenario_params_file.exists():
            raise ValueError(f"Scenario '{name}' does not exist")
        
        try:
            with open(scenario_params_file, 'r') as f:
                params = json.load(f)
        except json.JSONDecodeError:
            print(f"Warning: {name}_params.json is not valid JSON")
            # Use empty params as fallback
            params = {}
        
        # Create data source
        data_source = get_data_source("json", data_dir=str(self.data_dir))
        
        # Run simulation with parameter override
        simulation = SupplyChainSimulation(data_source, params)
        results = simulation.run()
        
        # Save results with scenario name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = self.scenarios_dir / f"{name}_results_{timestamp}.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        # Update metadata with this run
        metadata_file = self.metadata_dir / f"{name}.json"
        if metadata_file.exists():
            try:
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
            except json.JSONDecodeError:
                metadata = {"name": name}
        else:
            metadata = {"name": name}
        
        # Add run history
        if "runs" not in metadata:
            metadata["runs"] = []
        
        metadata["runs"].append({
            "timestamp": timestamp,
            "results_file": str(results_file),
            "overall_service_level": results.get("metrics", {}).get("overall_service_level", 0.0),
            "total_stockout_events": results.get("metrics", {}).get("total_stockout_events", 0)
        })
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"Completed simulation for scenario '{name}'")
        return results
    
    def list_scenarios(self) -> List[Dict[str, Any]]:
        """
        List all available scenarios with metadata.
        
        Returns:
            List of scenario information
        """
        scenarios = []
        
        for file in os.listdir(self.metadata_dir):
            if file.endswith(".json"):
                file_path = self.metadata_dir / file
                try:
                    with open(file_path, 'r') as f:
                        metadata = json.load(f)
                    scenarios.append(metadata)
                except json.JSONDecodeError:
                    print(f"Warning: {file} is not valid JSON")
        
        return scenarios
    
    def compare_scenarios(self, scenario_names: List[str]) -> Dict[str, Any]:
        """
        Compare multiple scenarios based on their latest results.
        
        Args:
            scenario_names: List of scenario names to compare
        
        Returns:
            Comparison data
        """
        comparison = {"scenarios": {}}
        
        for name in scenario_names:
            metadata_file = self.metadata_dir / f"{name}.json"
            if metadata_file.exists():
                try:
                    with open(metadata_file, 'r') as f:
                        metadata = json.load(f)
                    
                    # Get the latest run
                    if "runs" in metadata and metadata["runs"]:
                        latest_run = metadata["runs"][-1]
                        
                        comparison["scenarios"][name] = {
                            "description": metadata.get("description", "No description"),
                            "overall_service_level": latest_run.get("overall_service_level", 0),
                            "total_stockout_events": latest_run.get("total_stockout_events", 0),
                            "param_overrides": metadata.get("param_overrides", {})
                        }
                    else:
                        print(f"No runs found for scenario '{name}'")
                except json.JSONDecodeError:
                    print(f"Warning: {name}.json is not valid JSON")
            else:
                print(f"Scenario '{name}' not found")
        
        return comparison