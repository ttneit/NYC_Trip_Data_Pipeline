DROP DATABASE IF EXISTS data_warehouse;
CREATE DATABASE data_warehouse;
USE data_warehouse;


CREATE TABLE Dim_rate (
    RATECODEID Int64,
    RATE String,
    PRIMARY KEY (RATECODEID)
) ENGINE = MergeTree();


INSERT INTO Dim_rate VALUES (1, 'Standard rate'), 
                            (2, 'JFK'),
                            (3, 'Newark'),
                            (4, 'Nassau or Westchester'),
                            (5, 'Negotiated fare'),
                            (6, 'Group ride');


CREATE TABLE Dim_payment (
    Payment_typeID Int64,
    Payment_type String,
    PRIMARY KEY (Payment_typeID)
) ENGINE = MergeTree();


INSERT INTO Dim_payment VALUES (1, 'Credit card'),
                               (2, 'Cash'),
                               (3, 'No charge'),
                               (4, 'Dispute'),
                               (5, 'Unknown'),
                               (6, 'Voided trip');


CREATE TABLE Dim_trip (
    Trip_typeID Int64,
    Trip_type String,
    PRIMARY KEY (Trip_typeID)
) ENGINE = MergeTree();


INSERT INTO Dim_trip VALUES (0, 'Other'),
                            (1, 'Street-hail'),
                            (2, 'Dispatch');


CREATE TABLE Dim_flag (
    FlagID Int64,
    Flag_Code String,
    Flag String,
    PRIMARY KEY (FlagID)
) ENGINE = MergeTree();


INSERT INTO Dim_flag VALUES (1, 'Y', 'Store and forward trip'),
                            (2, 'N', 'Not a store and forward trip');


CREATE TABLE Dim_vendor (
    VendorID Int64,
    Vendor String,
    PRIMARY KEY (VendorID)
) ENGINE = MergeTree();


INSERT INTO Dim_vendor VALUES (1, 'Creative Mobile Technologies, LLC'),
                              (2, 'VeriFone Inc.');


CREATE TABLE green_fact_trips (
    VendorID Int64,  
    PickupDateID Int64,  
    DropoffDateID Int64,  
    RateCodeID Int64, 
    PaymentTypeID Int64, 
    TripTypeID Int64, 
    FlagID Int64, 
    PULocationID Int64,  
    DOLocationID Int64,  
    passenger_count Int64, 
    trip_distance Float64, 
    fare_amount Float64, 
    extra Float64, 
    mta_tax Float64, 
    tip_amount Float64, 
    tolls_amount Float64, 
    improvement_surcharge Float64, 
    total_amount Float64, 
    congestion_surcharge Float64, 
    airport_fee Float64, 
    pickup_datetime DateTime,  
    dropoff_datetime DateTime, 
    service_type String, 
    trip_duration Int64
) 
ENGINE = MergeTree()
ORDER BY pickup_datetime;


CREATE TABLE yellow_fact_trips (
    VendorID Int64,  
    PickupDateID Int64,  
    DropoffDateID Int64,  
    RateCodeID Int64, 
    PaymentTypeID Int64, 
    TripTypeID Int64, 
    FlagID Int64, 
    PULocationID Int64,  
    DOLocationID Int64,  
    passenger_count Int64, 
    trip_distance Float64, 
    fare_amount Float64, 
    extra Float64, 
    mta_tax Float64, 
    tip_amount Float64, 
    tolls_amount Float64, 
    improvement_surcharge Float64, 
    total_amount Float64, 
    congestion_surcharge Float64, 
    airport_fee Float64, 
    pickup_datetime DateTime,  
    dropoff_datetime DateTime, 
    service_type String, 
    trip_duration Int64
) 
ENGINE = MergeTree()
ORDER BY pickup_datetime;


CREATE TABLE Dim_date (
    DateTimeID Int64 PRIMARY KEY,
    DateTime DateTime,
    Year Int64,
    Month Int64,
    Day Int64,
    Hour Int64,
    DayOfWeek Int64,
    MonthName String,
    DayName String
) ENGINE = MergeTree();

INSERT INTO Dim_date FROM INFILE '/home/date_data.csv' FORMAT CSV;

CREATE TABLE Dim_location (
    LocationID Int64 PRIMARY KEY,
    Borough String,
    Zone String,
    service_zone String
) ENGINE = MergeTree();

INSERT INTO Dim_location FROM INFILE '/home/taxi_zone_lookup.csv' FORMAT CSV;