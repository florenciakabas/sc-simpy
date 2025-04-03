# simulation/scenario_manager.py
import ipdb
from typing import Dict, List, Any, Optional
import json
import os
from datetime import datetime
from pathlib import Path

from kedro.io import DataCatalog

from data.data_source import KedroCatalogWrapper
from simulation.engine import SupplyChainSimulation

import yaml


class ScenarioManager:
    """
    Utility for managing scenarios in the supply chain simulation.
    Uses Kedro's versioning capabilities for scenario management.
    """
    
    def __init__(self, catalog_config_path: str = 'catalog.yml'):
        """
        Initialize the scenario manager.
        
        Args:
            catalog_config_path: Path to the Kedro catalog config
        """
        self.catalog_config_path = catalog_config_path
        
        # Load catalog for metadata access
        with open(catalog_config_path, 'r') as f:
            catalog_dict = yaml.safe_load(f)
        
        # Find root data directory from any dataset filepath
        for dataset in catalog_dict.values():
            if "filepath" in dataset:
                data_dir = Path(dataset["filepath"]).parent
                break
        else:
            data_dir = Path("./data_files")
        
        # Initialize metadata directory
        self.metadata_dir = os.path.join(data_dir, ".kedro_metadata")
        os.makedirs(self.metadata_dir, exist_ok=True)
    
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
        # Create a data source with the base scenario
        data_source = KedroCatalogWrapper(
            catalog_config_path=self.catalog_config_path,
            scenario_name=base_scenario
        )
        
        # Get the simulation parameters
        params = data_source.get_simulation_params()
        
        # Apply overrides
        if param_overrides:
            for key, value in param_overrides.items():
                params[key] = value
        
        # Create a new data source for the target scenario
        target_data_source = KedroCatalogWrapper(
            catalog_config_path=self.catalog_config_path,
            scenario_name=name
        )
        
        # Load the catalog directly to save the parameters
        ipdb.set_trace()
        with open(self.catalog_config_path, 'r') as f:
            catalog_dict = json.load(f)
        catalog = DataCatalog.from_config(catalog_dict)
        
        # Save the parameters with the new scenario name
        catalog.save("simulation_params", params, version_name=name)
        
        # Add metadata
        self.journal.add_metadata("simulation_params", name, {
            "description": description,
            "base_scenario": base_scenario,
            "param_overrides": param_overrides,
            "created_at": datetime.now().isoformat()
        })
        
        print(f"Created scenario '{name}' based on '{base_scenario or 'latest'}'")
        return name
    
    def run_scenario(self, name: str) -> Dict[str, Any]:
        """
        Run a specific named scenario.
        
        Args:
            name: Name of the scenario to run
        
        Returns:
            Simulation results
        """
        # Create a data source with the specified scenario
        data_source = KedroCatalogWrapper(
            catalog_config_path=self.catalog_config_path,
            scenario_name=name
        )
        
        # Run the simulation
        simulation = SupplyChainSimulation(data_source)
        results = simulation.run()
        
        print(f"Completed simulation for scenario '{name}'")
        return results
    
    def list_scenarios(self) -> List[Dict[str, Any]]:
        """
        List all available scenarios with metadata.
        
        Returns:
            List of scenario information
        """
        # Get all versions of simulation_params
        try:
            metadata = self.journal.get_all_metadata("simulation_params")
            
            # Format the information
            scenarios = []
            for version_name, meta in metadata.items():
                scenarios.append({
                    "name": version_name,
                    "description": meta.get("description", "No description"),
                    "created_at": meta.get("created_at", "Unknown"),
                    "param_overrides": meta.get("param_overrides", {})
                })
            
            return scenarios
        except Exception as e:
            print(f"Error listing scenarios: {e}")
            return []
    
    def compare_scenarios(self, scenario_names: List[str]) -> Dict[str, Any]:
        """
        Compare multiple scenarios based on their results.
        
        Args:
            scenario_names: List of scenario names to compare
        
        Returns:
            Comparison data
        """
        comparison = {"scenarios": {}}
        
        for name in scenario_names:
            try:
                # Try to load results for this scenario
                with open(self.catalog_config_path, 'r') as f:
                    catalog_dict = json.load(f)
                catalog = DataCatalog.from_config(catalog_dict)
                
                # Find the latest results for this scenario
                results_versions = self.journal.get_all_versions("simulation_results")
                scenario_results = [v for v in results_versions if v.startswith(name)]
                
                if scenario_results:
                    # Get the latest result version
                    latest_result = sorted(scenario_results)[-1]
                    results = catalog.load("simulation_results", version_name=latest_result)
                    
                    # Extract key metrics
                    comparison["scenarios"][name] = {
                        "overall_service_level": results.get("metrics", {}).get("overall_service_level", 0),
                        "total_stockout_events": results.get("metrics", {}).get("total_stockout_events", 0),
                        "param_overrides": self.journal.get_metadata("simulation_params", name).get("param_overrides", {})
                    }
                else:
                    print(f"No results found for scenario '{name}'")
            except Exception as e:
                print(f"Error loading results for scenario '{name}': {e}")
        
        return comparison