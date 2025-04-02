# domain/entities.py
from typing import Dict, Any, List, Optional
from datetime import datetime
import math


class Ship:
    """Represents a shipping vessel in the supply chain."""
    
    def __init__(
        self,
        id: str,
        name: str,
        capacity: float,
        speed: float,
        initial_location: str,
        initial_cargo: float = 0.0
    ):
        """
        Initialize a ship.
        
        Args:
            id: Unique identifier
            name: Ship name
            capacity: Maximum cargo capacity
            speed: Speed in distance units per hour
            initial_location: Starting location
            initial_cargo: Starting cargo amount
        """
        self.id = id
        self.name = name
        self.capacity = capacity
        self.speed = speed
        self.current_location = initial_location
        self.current_cargo = min(initial_cargo, capacity)
        self.busy_until = 0.0  # Time until ship becomes available again
        self.travel_history = []
        
    def get_status(self) -> Dict[str, Any]:
        """Return the current status of the ship."""
        return {
            "id": self.id,
            "name": self.name,
            "location": self.current_location,
            "cargo": self.current_cargo,
            "available_capacity": self.capacity - self.current_cargo,
            "busy_until": self.busy_until
        }
        
    def load(self, amount: float) -> float:
        """
        Load cargo onto the ship.
        
        Args:
            amount: Amount to load
            
        Returns:
            Amount actually loaded
        """
        available_capacity = self.capacity - self.current_cargo
        actual_load = min(amount, available_capacity)
        self.current_cargo += actual_load
        return actual_load
        
    def unload(self, amount: float) -> float:
        """
        Unload cargo from the ship.
        
        Args:
            amount: Amount to unload
            
        Returns:
            Amount actually unloaded
        """
        actual_unload = min(amount, self.current_cargo)
        self.current_cargo -= actual_unload
        return actual_unload
    
    def calculate_travel_time(self, destination: str, distance_matrix: Dict[str, Dict[str, float]]) -> float:
        """
        Calculate travel time to destination.
        
        Args:
            destination: Destination location identifier
            distance_matrix: Matrix of distances between locations
            
        Returns:
            Travel time in hours
        """
        try:
            distance = distance_matrix[self.current_location][destination]
            return distance / self.speed
        except KeyError:
            raise ValueError(f"No route found from {self.current_location} to {destination}")
    
    def travel_to(self, destination: str, distance_matrix: Dict[str, Dict[str, float]], current_time: float) -> float:
        """
        Travel to a destination.
        
        Args:
            destination: Destination location
            distance_matrix: Matrix of distances between locations
            current_time: Current simulation time
            
        Returns:
            Arrival time
        """
        travel_time = self.calculate_travel_time(destination, distance_matrix)
        arrival_time = current_time + travel_time
        
        # Record the journey
        self.travel_history.append({
            "departure": self.current_location,
            "destination": destination,
            "departure_time": current_time,
            "arrival_time": arrival_time,
            "cargo": self.current_cargo
        })
        
        # Update ship state
        self.current_location = destination
        self.busy_until = arrival_time
        
        return arrival_time


class CustomerSite:
    """Represents a customer in the supply chain network."""
    
    def __init__(
        self,
        id: str,
        name: str,
        location: str,
        demand_rate: float,
        initial_inventory: float,
        min_inventory: float,
        max_inventory: float
    ):
        """
        Initialize a customer.
        
        Args:
            id: Unique identifier
            name: Customer name
            location: Customer location
            demand_rate: Rate of product consumption (units per hour)
            initial_inventory: Starting inventory level
            min_inventory: Minimum acceptable inventory level
            max_inventory: Maximum inventory capacity
        """
        self.id = id
        self.name = name
        self.location = location
        self.demand_rate = demand_rate
        self.current_inventory = initial_inventory
        self.min_inventory = min_inventory
        self.max_inventory = max_inventory
        self.inventory_history = []
        self.orders_history = []
        
    def get_status(self) -> Dict[str, Any]:
        """Return the current status of the customer."""
        return {
            "id": self.id,
            "name": self.name,
            "location": self.location,
            "inventory": self.current_inventory,
            "min_inventory": self.min_inventory,
            "inventory_deficit": max(0, self.min_inventory - self.current_inventory),
            "fill_capacity": self.max_inventory - self.current_inventory
        }
    
    def calculate_demand(self, time_period: float) -> float:
        """
        Calculate demand for a given time period.
        
        Args:
            time_period: Time period in hours
            
        Returns:
            Demand quantity
        """
        return self.demand_rate * time_period
    
    def consume(self, time_period: float, current_time: float) -> float:
        """
        Consume inventory over a time period.
        
        Args:
            time_period: Time period in hours
            current_time: Current simulation time
            
        Returns:
            Actual amount consumed
        """
        demand = self.calculate_demand(time_period)
        actual_consumption = min(demand, self.current_inventory)
        self.current_inventory -= actual_consumption
        
        # Record inventory level after consumption
        self.inventory_history.append({
            "time": current_time + time_period,
            "inventory": self.current_inventory,
            "demand": demand,
            "fulfilled": actual_consumption,
            "shortage": demand - actual_consumption if demand > actual_consumption else 0
        })
        
        return actual_consumption
    
    def receive_delivery(self, amount: float, current_time: float) -> float:
        """
        Receive a delivery of product.
        
        Args:
            amount: Amount delivered
            current_time: Current simulation time
            
        Returns:
            Amount actually received
        """
        available_space = self.max_inventory - self.current_inventory
        actual_received = min(amount, available_space)
        self.current_inventory += actual_received
        
        # Record the delivery
        self.orders_history.append({
            "time": current_time,
            "amount_requested": amount,
            "amount_received": actual_received,
            "inventory_after": self.current_inventory
        })
        
        return actual_received
    
    def days_of_supply(self) -> float:
        """
        Calculate days of supply based on current inventory and demand rate.
        
        Returns:
            Days of supply (inventory / daily demand)
        """
        if self.demand_rate <= 0:
            return float('inf')
        
        daily_demand = self.demand_rate * 24  # Convert hourly to daily
        return self.current_inventory / daily_demand if daily_demand > 0 else float('inf')