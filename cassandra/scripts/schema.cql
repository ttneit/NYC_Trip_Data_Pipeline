cqlsh

DROP KEYSPACE IF EXISTS nyc_taxi;

CREATE KEYSPACE nyc_taxi WITH REPLICATION = {'class' : 'SimpleStrategy' , 'replication_factor': 1};

USE nyc_taxi;

DROP TABLE IF EXISTS nyc_taxi.green_taxi;
DROP TABLE IF EXISTS nyc_taxi.yellow_taxi;

CREATE TABLE green_taxi (
    trip_id INT PRIMARY KEY,        
    VendorID INT,
    lpep_pickup_datetime TEXT,
    lpep_dropoff_datetime TEXT,
    store_and_fwd_flag TEXT,
    RatecodeID FLOAT,
    PULocationID INT,
    DOLocationID INT,
    passenger_count FLOAT,
    trip_distance FLOAT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    ehail_fee TEXT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    payment_type FLOAT,
    trip_type FLOAT,
    congestion_surcharge FLOAT
);

CREATE TABLE yellow_taxi (
    trip_id INT PRIMARY KEY,          
    VendorID INT,
    tpep_pickup_datetime TEXT,
    tpep_dropoff_datetime TEXT,
    passenger_count FLOAT,           
    trip_distance FLOAT,
    RatecodeID FLOAT,
    store_and_fwd_flag TEXT,
    PULocationID INT,
    DOLocationID INT,
    payment_type FLOAT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT
);

