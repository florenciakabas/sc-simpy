# domain/protocols.py
from typing import Protocol, List, Dict, Any, Optional
from datetime import datetime


class Resource(Protocol):
    """Protocol for any resource in the supply chain system."""
    
    id: str
    name: str
    
    def get_status(self) -> Dict[str, Any]:
        """Return the current status of the resource."""
        ...


class TransportVessel(Resource, Protocol):
    """Protocol defining capabilities of a transport vessel."""
    
    capacity: float
    speed: float
    current_location: str
    current_cargo: float
    
    def load(self, amount: float) -> float:
        """
        Load cargo onto the vessel.
        
        Args:
            amount: Amount to load
            
        Returns:
            Amount actually loaded (may be limited by capacity)
        """
        ...
    
    def unload(self, amount: float) -> float:
        """
        Unload cargo from the vessel.
        
        Args:
            amount: Amount to unload
            
        Returns:
            Amount actually unloaded
        """
        ...
    
    def calculate_travel_time(self, destination: str) -> float:
        """
        Calculate travel time to destination.
        
        Args:
            destination: Destination location identifier
            
        Returns:
            Travel time in hours
        """
        ...


class Customer(Resource, Protocol):
    """Protocol defining customer behavior."""
    
    location: str
    demand_rate: float
    current_inventory: float
    min_inventory: float
    max_inventory: float
    
    def calculate_demand(self, time_period: float) -> float:
        """
        Calculate demand for a given time period.
        
        Args:
            time_period: Time period in hours
            
        Returns:
            Demand quantity
        """
        ...
    
    def receive_delivery(self, amount: float) -> float:
        """
        Receive a delivery of product.
        
        Args:
            amount: Amount delivered
            
        Returns:
            Amount actually received
        """
        ...


class SimulationResult(Protocol):
    """Protocol for simulation results."""
    
    start_time: datetime
    end_time: datetime
    events: List[Dict[str, Any]]
    metrics: Dict[str, Any]
    
    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of the simulation results."""
        ...