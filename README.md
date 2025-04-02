supply_chain_sim/
│
├── domain/                 # Core domain models
│   ├── __init__.py
│   ├── entities.py         # Entity definitions (Ship, Customer, etc.)
│   └── protocols.py        # Protocol definitions
│
├── simulation/             # SimPy simulation implementation
│   ├── __init__.py
│   └── engine.py           # Simulation engine
│
├── data/                   # Data handling
│   ├── __init__.py
│   └── data_source.py  # Databricks integration
│
├── visualization/          # Visualization tools
│   ├── __init__.py
│   └── plotter.py          # Plotting utilities
│
├── config.py               # Configuration settings
├── main.py                 # Application entry point
└── requirements.txt        # Dependencies