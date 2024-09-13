from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import json
import time

def get_the_latest_trip_id(session):
    result = session.execute("SELECT MAX(trip_id) FROM green_taxi")
    max_trip_id = result.one()
    if max_trip_id[0] is None:  
        return 0
    else:
        return max_trip_id[0]




if __name__ == "__main__" : 
    print("-----------------------------------------------------------------------")
    print("Connect to keyspace nyc_taxi in Cssandra")
    cluster = Cluster(['localhost'])
    session = cluster.connect('nyc_taxi')

    print("-----------------------------------------------------------------------")
    print("Connect consumer to green-taxi topic in Kafka")
    consumer = KafkaConsumer(
        'green-taxi',
        group_id='group1',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: x.decode('utf-8')
    )

    print("-----------------------------------------------------------------------")
    print("Start streaming data in green-taxi topic")
    start_time = time.time()
    duration = 300
    current_id = get_the_latest_trip_id(session)

    while True:
        elapsed_time = time.time() - start_time
        if elapsed_time > duration:
            print(f"Stopping consumer after duration of {duration} seconds")
            break

        msg = consumer.poll(timeout_ms=1000)
        if msg:
            for tp, messages in msg.items():
                for message in messages:
                    query = """
                    INSERT INTO green_taxi (
                        trip_id, VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, store_and_fwd_flag, 
                        RatecodeID, PULocationID, DOLocationID, passenger_count, trip_distance, fare_amount, 
                        extra, mta_tax, tip_amount, tolls_amount, ehail_fee, improvement_surcharge, 
                        total_amount, payment_type, trip_type, congestion_surcharge
                    ) 
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """
                    data_dict = json.loads(message.value)
                    try:
                        data = (
                            current_id, 
                            data_dict.get('VendorID'), 
                            data_dict.get('lpep_pickup_datetime'), 
                            data_dict.get('lpep_dropoff_datetime'), 
                            data_dict.get('store_and_fwd_flag'), 
                            (data_dict.get('RatecodeID', 0)) ,
                            data_dict.get('PULocationID'), 
                            data_dict.get('DOLocationID'),
                            data_dict.get('passenger_count'), 
                            data_dict.get('trip_distance'), 
                            data_dict.get('fare_amount'), 
                            data_dict.get('extra'), 
                            data_dict.get('mta_tax'), 
                            data_dict.get('tip_amount'), 
                            data_dict.get('tolls_amount'), 
                            data_dict.get('ehail_fee'),  
                            data_dict.get('improvement_surcharge'),
                            data_dict.get('total_amount'),
                            (data_dict.get('payment_type', 0)) ,
                            (data_dict.get('trip_type', 0)) ,
                            data_dict.get('congestion_surcharge')
                        )
                        print(data)
                        session.execute(query, data)
                        print("Inserted new data successfully")
                        current_id += 1
                    except Exception as e:
                        print("Error inserting data:", str(e))
        else:
            print("There is no new data in green-taxi topic")
            time.sleep(2)
    print("-----------------------------------------------------------------------")
    print("Commit offsets ")
    consumer.commit()
    print("-----------------------------------------------------------------------")
    print("Close consumer")
    consumer.close()