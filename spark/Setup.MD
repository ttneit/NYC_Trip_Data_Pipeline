# How to run PySpark in Docker 

# Step 1 : Create a `docker-compose.yml` file for PySpark 
[YAML file](docker-compose.yml)

# Step 2 : Run the `docker-compose.yml` file
Run this command : 
```
docker-compose up -d
```
# Step 3 : Copy needed jars file to Docker Container
```
docker cp mssql-jdbc-12.6.0.jre8.jar spark-spark-1:/opt/bitnami/spark/jars/
```

# Step 4 : Execute spark job
1. For `green_etl.py`
```
docker exec -it spark-spark-1 /opt/bitnami/spark/bin/spark-submit 
    --master spark://spark-spark-1:7077 
    --deploy-mode client 
    --executor-memory 4G 
    --total-executor-cores 4 
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 
    --jars /opt/bitnami/spark/jars/mssql-jdbc-12.6.0.jre8.jar 
    /opt/application/scripts/green_etl.py
```
2. For `yellow_etl.py`
```
docker exec -it spark-spark-1 /opt/bitnami/spark/bin/spark-submit 
    --master spark://spark-spark-1:7077 
    --deploy-mode client 
    --executor-memory 4G 
    --total-executor-cores 4 
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 
    --jars /opt/bitnami/spark/jars/mssql-jdbc-12.6.0.jre8.jar 
    /opt/application/scripts/yellow_etl.py
```

# Reference

- [PySpark SQL API Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html)
- [How to set up Spark in Docker](https://medium.com/programmers-journey/deadsimple-pyspark-docker-spark-cluster-on-your-laptop-9f12e915ecf4)