# NYC Trip Record Data Pipeline

## I. Introduce
### Goal of project
With the purpose of learning and practicing about Data Engineer and BigData, I have done this project. This project focuses on processing data from raw data sources (Parquet files) to data lake (Apache Cassandra) by simulating streaming data through Apache Kafka (This simulation is designed for the purpose of performing near-realtime process). Then, proceed to implement ETL Pipeline from Data Lake to Data Warehouse (SQL Server). Finally, use PowerBI to visualize the data and from there provide perspectives on NYC Trip Record. In this stage , the project does not handle near-realtime process (I will learn to schedule PySpark Job by Apache Airflow for near-realtime processing)

### Data Sources 
Raw Data Sources is selected from  Public dataset `TLC Trip Record Data` . The dataset includes 2 types of taxi :
- `Yellow Taxi (Yellow Medallion Taxicabs)` : These are the famous NYC yellow taxis that provide transportation exclusively through street hails. The number of taxicabs is limited by a finite number of medallions issued by the TLC. You access this mode of transportation by standing in the street and hailing an available taxi with your hand. The pickups are not pre-arranged. 
- `Green Taxi (Street Hail Livery)` : The SHL program will allow livery vehicle owners to license and outfit their vehicles with green borough taxi branding, meters, credit card machines, and ultimately the right to accept street hails in addition to pre-arranged rides.

Dataset in project : 1 year TLC Trip Record (2023) 
[Dataset Page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
[Taxi Zone Lookup Table](raw_data/taxi_zone_lookup.csv)
[Yellow Taxi Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
[Green Taxi Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf)


## II. Architecture
### Directory Tree 
- `cassandra` : Contains schema file in DataLake and the way to setup Cassandra in Docker
- `kafka` : Contains 2 subdirectories for producer and consumer and the way to setup Kafka in Docker-Compose
- `raw_data` : Contains raw data in parquet format files
- `images` : Contains images which is necessary for `README.MD`
- `mssql` : Contains schema file in Data Warehouse 
- `kafka` : Contains ETL pipelines and the way to setup PySpark in Docker-Compose
- `visualization` : Contains dashboard file in PowerBI
- `README.MD` : Reports 

### Project Architecture 
![Architecture](images/architecture.png)

### Schema in Data Lake
1. Green Taxi : 

![Green Taxi](images/green_taxi.png)

2. Yellow Taxi : 

![Yellow Taxi](images/yellow_taxi.png)

### Schema in Data Warehouse
Data Warehouse Schema : 
![Data Warehouse](images/DW.png)


## III. Visualization
1. Overview : 

![Overview](images/Overview.png)

2. Location : 

![Location](images/Location.png)

3. Location : 

![Location(1)](images/Location_1.png)

4. Revenue : 

![Revenue](images/Revenue.png)

5. Revenue : 

![Revenue(1)](images/Revenue_1.png)

6. User Preferences : 

![User](images/User.png)