# How to run Kafka in Docker 

# Step 1 : Create a `docker-compose.yml` file for kafka including two services : zookeeper and kafka
[YAML file](config/docker-compose.yml)

# Step 2 : Run the `docker-compose.yml` file
Run this command : 
```
docker-compose up -d
```

# Step 3 : Create topic in Kafka
1. Open shell in the Kafka container
```
docker exec -it kafka-kafka-1 /bin/bash
```
2. Create topics 
```
kafka-topics.sh --create --topic green-taxi --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
kafka-topics.sh --create --topic yellow-taxi --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
```

# Step 4 : Run scripts to simulate streaming data for 1 day foe each type of taxi
1. Green taxi : 
```
python consumer/consumer_green.py
```

```
python producer/producer_green.py
```
2. Yellow taxi : 
```
python consumer/consumer_yellow.py
```

```
python producer/producer_yellow.py
```
# Reference

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/#introduction)
- [How to set up Kafka (1)](https://www.startdataengineering.com/post/what-why-and-how-apache-kafka/)
- [How to set up Kafka (2)](https://www.digitalocean.com/community/developer-center/how-to-deploy-kafka-on-docker-and-digitalocean-kubernetes)