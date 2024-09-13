# How to run ClickHouseDB in Docker

## Step 1 : Get ClickHouseDB using Docker
Use this command to get the docker image of Cassandra
```
docker pull clickhouse/clickhouse-server
```
## Step 2 : Start ClickHouseDB 
```
docker run -d --name clickhouse-server --network nyc-network -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server
```

## Step 3 : Run SQL scripts to create dim tables and fact tables in ClickHouseDB
1. Access ClickHouseDB Client
```
docker exec -it clickhouse-server clickhouse-client
```
2. Copy `date_date.csv` and `taxi_zone_lookup.csv`from local to Docker 
```
docker cp ../raw_data/date_data.csv clickhouse-server:/home/date_data.csv
docker cp ../raw_data/taxi_zone_lookup.csv clickhouse-server:/home/taxi_zone_lookup.csv
```

3. Run SQL scripts to create dim tables and fact tables in ClickHouseDB
[Schema](scripts/schema.sql)

# Reference :
[ClickHouseDB Docker](https://hub.docker.com/r/clickhouse/clickhouse-server/)
[ClickHouseDB Document](https://clickhouse.com/docs/en/intro)
