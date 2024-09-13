DROP DATABASE IF EXISTS nyc_taxi;

CREATE DATABASE nyc_taxi; 
USE nyc_taxi;

DROP TABLE IF EXISTS Dim_rate;
DROP TABLE IF EXISTS Dim_payment;
DROP TABLE IF EXISTS Dim_trip;
DROP TABLE IF EXISTS Dim_flag;
DROP TABLE IF EXISTS Dim_vendor;
DROP TABLE IF EXISTS Dim_date;
DROP TABLE IF EXISTS Dim_zone;

DROP TABLE IF EXISTS green_fact_trips;
DROP TABLE IF EXISTS yellow_fact_trips;

CREATE TABLE Dim_rate (
	RATECODEID INT PRIMARY KEY,
	RATE NVARCHAR(50)
);
INSERT INTO Dim_rate (RATECODEID, RATE) VALUES (1, 'Standard rate');
INSERT INTO Dim_rate (RATECODEID, RATE) VALUES (2, 'JFK');
INSERT INTO Dim_rate (RATECODEID, RATE) VALUES (3, 'Newark');
INSERT INTO Dim_rate (RATECODEID, RATE) VALUES (4, 'Nassau or Westchester');
INSERT INTO Dim_rate (RATECODEID, RATE) VALUES (5, 'Negotiated fare');
INSERT INTO Dim_rate (RATECODEID, RATE) VALUES (6, 'Group ride');
create table Dim_payment (
	Payment_typeID INT PRIMARY KEY,
	Payment_type NVARCHAR(50)
);
INSERT INTO Dim_payment (Payment_typeID, Payment_type) VALUES (1, 'Credit card');
INSERT INTO Dim_payment (Payment_typeID, Payment_type) VALUES (2, 'Cash');
INSERT INTO Dim_payment (Payment_typeID, Payment_type) VALUES (3, 'No charge');
INSERT INTO Dim_payment (Payment_typeID, Payment_type) VALUES (4, 'Dispute');
INSERT INTO Dim_payment (Payment_typeID, Payment_type) VALUES (5, 'Unknown');
INSERT INTO Dim_payment (Payment_typeID, Payment_type) VALUES (6, 'Voided trip');
CREATE TABLE Dim_trip (
	Trip_typeID INT PRIMARY KEY ,
	Trip_type NVARCHAR(50)
);
INSERT INTO Dim_trip (Trip_typeID, Trip_type) VALUES (0, 'Other');
INSERT INTO Dim_trip (Trip_typeID, Trip_type) VALUES (1, 'Street-hail');
INSERT INTO Dim_trip (Trip_typeID, Trip_type) VALUES (2, 'Dispatch');
CREATE TABLE Dim_flag (
	FlagID INT PRIMARY KEY ,
	Flag_Code VARCHAR(1),
	Flag NVARCHAR(50)
);

INSERT INTO Dim_flag (FlagID, Flag_Code, Flag) VALUES (1, 'Y', 'Store and forward trip');
INSERT INTO Dim_flag (FlagID, Flag_Code, Flag) VALUES (2, 'N', 'Not a store and forward trip');

CREATE TABLE Dim_vendor (
	VendorID INT PRIMARY KEY ,
	Vendor NVARCHAR(50)
);
INSERT INTO Dim_vendor (VendorID, Vendor) VALUES (1, 'Creative Mobile Technologies, LLC');
INSERT INTO Dim_vendor (VendorID, Vendor) VALUES (2, 'VeriFone Inc.');

CREATE TABLE Dim_date (
    DateTimeID INT IDENTITY(1,1) PRIMARY KEY,
    DateTime DATETIME,
    Year INT,
    Month INT,
    Day INT,
    Hour INT,
    DayOfWeek INT,
    MonthName NVARCHAR(50),
    DayName NVARCHAR(50)
);

DECLARE @StartDateTime DATETIME = '2023-01-01 00:00:00';
DECLARE @EndDateTime DATETIME = '2023-12-31 23:00:00';

WHILE @StartDateTime <= @EndDateTime
BEGIN
    INSERT INTO Dim_date (DateTime, Year, Month, Day, Hour, DayOfWeek, MonthName, DayName)
    VALUES (
        @StartDateTime,
        YEAR(@StartDateTime),
        MONTH(@StartDateTime),
        DAY(@StartDateTime),
        DATEPART(HOUR, @StartDateTime),
        DATEPART(WEEKDAY, @StartDateTime),
        DATENAME(MONTH, @StartDateTime),
        DATENAME(WEEKDAY, @StartDateTime)
    );

    SET @StartDateTime = DATEADD(HOUR, 1, @StartDateTime);
END;



CREATE TABLE Dim_zone (
	LocationID INT PRIMARY KEY,
	Borough NVARCHAR(50),
	Zone NVARCHAR(50),
	service_zone NVARCHAR(50)
);
BULK INSERT Dim_zone
FROM 'D:\Github\nyc_taxi_pipeline\Local_version\raw_data\taxi_zone_lookup.csv'
WITH
(
    FORMAT='CSV',
    FIRSTROW=2
);

CREATE TABLE fact_trips (
    trip_id INT IDENTITY(1,1) PRIMARY KEY,
	VendorID INT FOREIGN KEY REFERENCES Dim_vendor(VendorID),  
    PickupDateID INT FOREIGN KEY REFERENCES Dim_date(DateTimeID),  
    DropoffDateID INT FOREIGN KEY REFERENCES Dim_date(DateTimeID),  
    RateCodeID INT FOREIGN KEY REFERENCES Dim_rate(RATECODEID), 
    PaymentTypeID INT FOREIGN KEY REFERENCES Dim_payment(Payment_typeID), 
    TripTypeID INT FOREIGN KEY REFERENCES Dim_trip(Trip_typeID), 
    FlagID INT FOREIGN KEY REFERENCES Dim_flag(FlagID), 
	PULocationID INT,  
    DOLocationID INT,  
    passenger_count INT, 
    trip_distance FLOAT, 
    fare_amount FLOAT, 
    extra FLOAT, 
    mta_tax FLOAT, 
    tip_amount FLOAT, 
    tolls_amount FLOAT, 
    improvement_surcharge FLOAT, 
    total_amount FLOAT, 
    congestion_surcharge FLOAT, 
	airport_fee FLOAT, 
    pickup_datetime DATETIME,  
    dropoff_datetime DATETIME, 
	service_type VARCHAR(50), 
    trip_duration AS DATEDIFF(MINUTE, pickup_datetime, dropoff_datetime)
);


ALTER TABLE fact_trips
ADD CONSTRAINT FK_fact_PU
FOREIGN KEY (PULocationID) REFERENCES Dim_zone(LocationID);

ALTER TABLE fact_trips
ADD CONSTRAINT FK_fact_DO
FOREIGN KEY (DOLocationID) REFERENCES Dim_zone(LocationID);
