# How to setup Apache Cassandra in Docker

## Step 1 : Get Cassandra using Docker
Use this command to get the docker image of Cassandra
```
docker pull cassandra:latest
```
## Step 2 : Start Cassandra in nyc-network
```
docker run -d --name my-cassandra -p 9042:9042  --hostname cassandra -e CASSANDRA_CLUSTER_NAME=data_lake -e CASSANDRA_USER=cassandra -e CASSANDRA_PASSWORD=cassandra  --network nyc-network cassandra
```

## Step 3 : Run CQL scripts to create database in Cassandra
[Schema](scripts/schema.cql)

# Reference :
[Cassandra Document](https://cassandra.apache.org/_/quickstart.html)
[CQL Command](https://docs.datastax.com/en/cql-oss/3.x/cql/cql_reference/cqlCommandsTOC.html)