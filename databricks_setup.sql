-- databricks_setup.sql

-- Create ships table
CREATE TABLE IF NOT EXISTS ships (
    id STRING,
    name STRING,
    capacity DOUBLE,
    speed DOUBLE,
    initial_location STRING,
    initial_cargo DOUBLE
);

-- Insert sample ship data
INSERT INTO ships VALUES 
    ('ship_1', 'Vessel Alpha', 100000.0, 25.0, 'port_main', 80000.0),
    ('ship_2', 'Vessel Beta', 75000.0, 30.0, 'port_main', 60000.0),
    ('ship_3', 'Vessel Gamma', 120000.0, 20.0, 'port_main', 100000.0);

-- Create customers table
CREATE TABLE IF NOT EXISTS customers (
    id STRING,
    name STRING,
    location STRING,
    demand_rate DOUBLE,
    initial_inventory DOUBLE,
    min_inventory DOUBLE,
    max_inventory DOUBLE
);

-- Insert sample customer data
INSERT INTO customers VALUES
    ('customer_1', 'Manufacturing Plant A', 'location_a', 1000.0, 48000.0, 24000.0, 120000.0),
    ('customer_2', 'Distribution Center B', 'location_b', 750.0, 36000.0, 18000.0, 90000.0),
    ('customer_3', 'Processing Facility C', 'location_c', 1200.0, 57600.0, 28800.0, 144000.0);

-- Create distances table
CREATE TABLE IF NOT EXISTS distances (
    from_location STRING,
    to_location STRING, 
    distance DOUBLE
);

-- Insert sample distance data (you may need to adjust these values)
INSERT INTO distances VALUES
    ('port_main', 'port_main', 0.0),
    ('port_main', 'location_a', 450.0),
    ('port_main', 'location_b', 600.0),
    ('port_main', 'location_c', 750.0),
    ('location_a', 'port_main', 450.0),
    ('location_a', 'location_a', 0.0),
    ('location_a', 'location_b', 250.0),
    ('location_a', 'location_c', 400.0),
    ('location_b', 'port_main', 600.0),
    ('location_b', 'location_a', 250.0),
    ('location_b', 'location_b', 0.0),
    ('location_b', 'location_c', 300.0),
    ('location_c', 'port_main', 750.0),
    ('location_c', 'location_a', 400.0),
    ('location_c', 'location_b', 300.0),
    ('location_c', 'location_c', 0.0);

-- Create simulation parameters table
CREATE TABLE IF NOT EXISTS simulation_params (
    param_name STRING,
    param_value STRING,
    param_type STRING
);

-- Insert sample simulation parameters
INSERT INTO simulation_params VALUES
    ('simulation_duration', '720.0', 'float'),
    ('time_step', '1.0', 'float'),
    ('resupply_threshold_days', '3.0', 'float'),
    ('loading_rate', '5000.0', 'float'),
    ('unloading_rate', '4000.0', 'float'),
    ('port_resupply_delay', '12.0', 'float'),
    ('random_seed', '42', 'int');

-- Create simulation results table
CREATE TABLE IF NOT EXISTS simulation_results (
    run_id STRING,
    timestamp STRING,
    overall_service_level DOUBLE,
    results_json STRING
);