# simulation/engine.py
import simpy
import random
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import pandas as pd

from domain.entities import Ship, CustomerSite
from data.data_source import DataSource


class SupplyChainSimulation:
    """
    SimPy-based discrete event simulation for a supply chain with ships and customers.
    """
    
    def __init__(
        self, 
        data_source: DataSource,
        param_override: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the simulation with configuration from the data source.
        
        Args:
            data_source: Data source for configuration
            param_override: Optional dictionary to override specific simulation parameters
        """
        # Load configuration from data source
        self.ships_data = data_source.get_ships_data()
        self.customers_data = data_source.get_customers_data()
        self.distance_matrix = data_source.get_distance_matrix()
        self.simulation_params = data_source.get_simulation_params()
        
        # Override parameters if provided
        if param_override:
            for key, value in param_override.items():
                self.simulation_params[key] = value
        
        # Initialize SimPy environment
        self.env = None
        self.ships = {}
        self.customers = {}
        
        # Store simulation results
        self.results = {
            "events": [],
            "metrics": {},
            "ships_history": [],
            "customers_history": []
        }
        
        self.data_source = data_source
    
    def setup(self):
        """Set up the simulation environment and entities."""
        # Create a new SimPy environment
        random.seed(self.simulation_params.get("random_seed", 42))
        self.env = simpy.Environment()
        
        # Create ships
        self.ships = {}
        for ship_data in self.ships_data:
            ship = Ship(
                id=ship_data["id"],
                name=ship_data["name"],
                capacity=ship_data["capacity"],
                speed=ship_data["speed"],
                initial_location=ship_data["initial_location"],
                initial_cargo=ship_data.get("initial_cargo", 0.0)
            )
            self.ships[ship.id] = ship
        
        # Create customers
        self.customers = {}
        for customer_data in self.customers_data:
            customer = CustomerSite(
                id=customer_data["id"],
                name=customer_data["name"],
                location=customer_data["location"],
                demand_rate=customer_data["demand_rate"],
                initial_inventory=customer_data["initial_inventory"],
                min_inventory=customer_data["min_inventory"],
                max_inventory=customer_data["max_inventory"]
            )
            self.customers[customer.id] = customer
        
        # Clear previous results
        self.results = {
            "events": [],
            "metrics": {},
            "ships_history": [],
            "customers_history": []
        }
    
    def log_event(self, time: float, event_type: str, details: Dict[str, Any]):
        """
        Log a simulation event.
        
        Args:
            time: Simulation time
            event_type: Type of event
            details: Event details
        """
        event = {
            "time": time,
            "type": event_type,
            **details
        }
        self.results["events"].append(event)
    
    def customer_consumption_process(self, customer_id: str):
        """
        Process that simulates customer inventory consumption over time.
        
        Args:
            customer_id: ID of the customer
        """
        customer = self.customers[customer_id]
        time_step = self.simulation_params["time_step"]
        
        while True:
            # Wait for the next time step
            yield self.env.timeout(time_step)
            
            # Consume inventory
            consumed = customer.consume(time_step, self.env.now)
            
            # Log consumption event
            self.log_event(
                time=self.env.now,
                event_type="consumption",
                details={
                    "customer_id": customer_id,
                    "customer_name": customer.name,
                    "amount_consumed": consumed,
                    "current_inventory": customer.current_inventory,
                    "days_of_supply": customer.days_of_supply()
                }
            )
            
            # Check if we need to trigger a delivery
            threshold_days = self.simulation_params.get("resupply_threshold_days", 3.0)
            if customer.days_of_supply() < threshold_days:
                # Find an available ship with cargo
                self.schedule_delivery(customer_id)
    
    def schedule_delivery(self, customer_id: str):
        """
        Schedule a delivery to a customer when inventory is low.
        
        Args:
            customer_id: ID of the customer needing delivery
        """
        customer = self.customers[customer_id]
        
        # Calculate how much the customer needs
        target_inventory = customer.max_inventory * 0.8  # Target 80% of max capacity
        needed_amount = target_inventory - customer.current_inventory
        
        if needed_amount <= 0:
            return  # No delivery needed
        
        # Find available ships with cargo
        available_ships = [
            ship for ship in self.ships.values()
            if ship.current_cargo > 0 and ship.busy_until <= self.env.now
        ]
        
        if not available_ships:
            # Log that no ships are available
            self.log_event(
                time=self.env.now,
                event_type="delivery_failed",
                details={
                    "customer_id": customer_id,
                    "reason": "no_ships_available",
                    "needed_amount": needed_amount
                }
            )
            return
        
        # Find the ship with the most cargo
        ship = max(available_ships, key=lambda s: s.current_cargo)
        
        # Start the delivery process
        self.env.process(self.delivery_process(ship.id, customer_id, needed_amount))
    
    def delivery_process(self, ship_id: str, customer_id: str, amount: float):
        """
        Process that handles a ship delivering to a customer.
        
        Args:
            ship_id: ID of the ship
            customer_id: ID of the customer
            amount: Amount to deliver
        """
        ship = self.ships[ship_id]
        customer = self.customers[customer_id]
        
        # Log delivery start
        self.log_event(
            time=self.env.now,
            event_type="delivery_started",
            details={
                "ship_id": ship_id,
                "ship_name": ship.name,
                "customer_id": customer_id,
                "customer_name": customer.name,
                "requested_amount": amount,
                "available_cargo": ship.current_cargo
            }
        )
        
        # Travel to customer location
        travel_time = ship.travel_to(customer.location, self.distance_matrix, self.env.now)
        yield self.env.timeout(travel_time - self.env.now)  # Adjust for current time
        
        # Log arrival at customer
        self.log_event(
            time=self.env.now,
            event_type="ship_arrived",
            details={
                "ship_id": ship_id,
                "ship_name": ship.name,
                "location": customer.location,
                "customer_id": customer_id,
                "customer_name": customer.name
            }
        )
        
        # Unload cargo
        delivery_amount = min(amount, ship.current_cargo)
        unloading_time = delivery_amount / self.simulation_params.get("unloading_rate", 4000.0)
        yield self.env.timeout(unloading_time)
        
        actual_unloaded = ship.unload(delivery_amount)
        actual_received = customer.receive_delivery(actual_unloaded, self.env.now)
        
        # Log delivery completion
        self.log_event(
            time=self.env.now,
            event_type="delivery_completed",
            details={
                "ship_id": ship_id,
                "ship_name": ship.name,
                "customer_id": customer_id,
                "customer_name": customer.name,
                "amount_delivered": actual_received,
                "customer_inventory": customer.current_inventory,
                "ship_remaining_cargo": ship.current_cargo
            }
        )
        
        # If ship is low on cargo, return to port for resupply
        if ship.current_cargo < 0.2 * ship.capacity:
            self.env.process(self.ship_resupply_process(ship_id))
        else:
            # Make ship available for other deliveries
            ship.busy_until = self.env.now
    
    def ship_resupply_process(self, ship_id: str):
        """
        Process for ship returning to port for resupply.
        
        Args:
            ship_id: ID of the ship
        """
        ship = self.ships[ship_id]
        port_location = "port_main"  # Assuming one main port
        
        # Log resupply start
        self.log_event(
            time=self.env.now,
            event_type="resupply_started",
            details={
                "ship_id": ship_id,
                "ship_name": ship.name,
                "current_location": ship.current_location,
                "destination": port_location,
                "current_cargo": ship.current_cargo
            }
        )
        
        # Travel to port
        if ship.current_location != port_location:
            travel_time = ship.travel_to(port_location, self.distance_matrix, self.env.now)
            yield self.env.timeout(travel_time - self.env.now)  # Adjust for current time
        
        # Log arrival at port
        self.log_event(
            time=self.env.now,
            event_type="ship_arrived",
            details={
                "ship_id": ship_id,
                "ship_name": ship.name,
                "location": port_location
            }
        )
        
        # Wait for resupply processing
        resupply_delay = self.simulation_params.get("port_resupply_delay", 12.0)
        yield self.env.timeout(resupply_delay)
        
        # Load new cargo
        available_capacity = ship.capacity - ship.current_cargo
        loading_time = available_capacity / self.simulation_params.get("loading_rate", 5000.0)
        yield self.env.timeout(loading_time)
        
        ship.load(available_capacity)
        
        # Log resupply completion
        self.log_event(
            time=self.env.now,
            event_type="resupply_completed",
            details={
                "ship_id": ship_id,
                "ship_name": ship.name,
                "location": port_location,
                "new_cargo_level": ship.current_cargo
            }
        )
        
        # Make ship available for deliveries
        ship.busy_until = self.env.now
    
    def run(self):
        """Run the simulation."""
        # Set up the simulation
        self.setup()
        
        # Record simulation metadata
        start_time = datetime.now()
        self.results["metadata"] = {
            "start_time": start_time.isoformat(),
            "params": self.simulation_params,
            "num_ships": len(self.ships),
            "num_customers": len(self.customers)
        }
        
        # Start customer consumption processes
        for customer_id in self.customers:
            self.env.process(self.customer_consumption_process(customer_id))
        
        # Run the simulation for the specified duration
        sim_duration = self.simulation_params["simulation_duration"]
        self.env.run(until=sim_duration)
        
        # Update simulation metadata
        end_time = datetime.now()
        self.results["metadata"]["end_time"] = end_time.isoformat()
        self.results["metadata"]["duration_seconds"] = (end_time - start_time).total_seconds()
        
        # Calculate and store metrics
        self._calculate_metrics()
        
        # Extract history for visualization
        self._extract_history()
        
        # Save results
        self.data_source.save_results(self.results)
        
        return self.results
    
    def _calculate_metrics(self):
        """Calculate key performance metrics from simulation results."""
        # Initialize metrics dictionary
        metrics = {}
        
        # Process events to extract metrics
        events_df = pd.DataFrame(self.results["events"])
        
        # Customer service metrics
        if not events_df.empty and 'type' in events_df.columns:
            # Calculate stockout events (when inventory reaches zero)
            stockout_events = events_df[
                (events_df["type"] == "consumption") & 
                (events_df["current_inventory"] == 0)
            ]
            metrics["total_stockout_events"] = len(stockout_events)
            
            # Group by customer to get customer-specific metrics
            customer_metrics = {}
            for customer_id, customer in self.customers.items():
                customer_consumption = events_df[
                    (events_df["type"] == "consumption") & 
                    (events_df["customer_id"] == customer_id)
                ]
                
                if not customer_consumption.empty:
                    # Calculate average inventory level
                    avg_inventory = customer_consumption["current_inventory"].mean()
                    min_inventory = customer_consumption["current_inventory"].min()
                    
                    # Calculate stockout duration
                    stockouts = customer_consumption[customer_consumption["current_inventory"] == 0]
                    stockout_hours = len(stockouts) * self.simulation_params["time_step"]
                    
                    # Calculate service level (% of time with inventory > 0)
                    service_level = 1.0 - (stockout_hours / self.simulation_params["simulation_duration"])
                    
                    customer_metrics[customer_id] = {
                        "avg_inventory": avg_inventory,
                        "min_inventory": min_inventory,
                        "stockout_hours": stockout_hours,
                        "service_level": service_level
                    }
            
            metrics["customer_metrics"] = customer_metrics
        
        # Ship utilization metrics
        ship_metrics = {}
        for ship_id, ship in self.ships.items():
            ship_events = events_df[
                (events_df["type"].isin(["delivery_started", "delivery_completed", "resupply_started", "resupply_completed"])) &
                (events_df["ship_id"] == ship_id)
            ]
            
            if not ship_events.empty:
                # Calculate total distance traveled
                total_distance = 0
                for journey in ship.travel_history:
                    from_loc = journey["departure"]
                    to_loc = journey["destination"]
                    if from_loc in self.distance_matrix and to_loc in self.distance_matrix[from_loc]:
                        total_distance += self.distance_matrix[from_loc][to_loc]
                
                ship_metrics[ship_id] = {
                    "total_distance": total_distance,
                    "num_deliveries": len(ship_events[ship_events["type"] == "delivery_completed"]),
                    "num_resupplies": len(ship_events[ship_events["type"] == "resupply_completed"])
                }
        
        metrics["ship_metrics"] = ship_metrics
        
        # Overall system metrics
        if "customer_metrics" in metrics:
            overall_service_level = sum(
                m["service_level"] for m in metrics["customer_metrics"].values()
            ) / len(metrics["customer_metrics"])
            
            metrics["overall_service_level"] = overall_service_level
        
        # Store the calculated metrics
        self.results["metrics"] = metrics
    
    def _extract_history(self):
        """Extract time series data for visualization."""
        # Extract ship history
        ships_history = []
        for ship_id, ship in self.ships.items():
            for journey in ship.travel_history:
                ships_history.append({
                    "ship_id": ship_id,
                    "ship_name": ship.name,
                    "departure": journey["departure"],
                    "destination": journey["destination"],
                    "departure_time": journey["departure_time"],
                    "arrival_time": journey["arrival_time"],
                    "cargo": journey["cargo"]
                })
        
        # Extract customer inventory history
        customers_history = []
        for customer_id, customer in self.customers.items():
            for record in customer.inventory_history:
                customers_history.append({
                    "customer_id": customer_id,
                    "customer_name": customer.name,
                    "time": record["time"],
                    "inventory": record["inventory"],
                    "demand": record["demand"],
                    "fulfilled": record["fulfilled"],
                    "shortage": record["shortage"]
                })
        
        self.results["ships_history"] = ships_history
        self.results["customers_history"] = customers_history


class ParameterStudy:
    """
    Utility for studying the effect of varying a parameter in the simulation.
    """
    
    def __init__(
        self, 
        data_source: DataSource,
        param_name: str,
        param_values: List[Any]
    ):
        """
        Initialize a parameter study.
        
        Args:
            data_source: Data source for simulation configuration
            param_name: Name of the parameter to vary
            param_values: List of values to use for the parameter
        """
        self.data_source = data_source
        self.param_name = param_name
        self.param_values = param_values
        self.results = []
    
    def run(self):
        """Run the parameter study and return results."""
        for value in self.param_values:
            print(f"Running simulation with {self.param_name} = {value}")
            
            # Create parameter override
            param_override = {self.param_name: value}
            
            # Run simulation with this parameter value
            simulation = SupplyChainSimulation(self.data_source, param_override)
            result = simulation.run()
            
            # Store key metrics with parameter value
            self.results.append({
                "param_name": self.param_name,
                "param_value": value,
                "metrics": result["metrics"]
            })
        
        return self.results