from kafka import KafkaProducer
import pyarrow.parquet as pq
import os
import json
import time
import datetime
import re
def insert_data_into_kafka(directory: str ,cur_file_name : str , producer : KafkaProducer) :
    print(directory)
    topic_name = "green-taxi"
    
    parquet_df = pq.read_table(directory+"/"+cur_file_name)
    df = parquet_df.to_pandas()
    df['lpep_pickup_datetime'] = df['lpep_pickup_datetime'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    
    for _, row in df.iterrows() : 
        producer.send(topic_name, value=row.to_dict())
    print(f"Finish inserting {topic_name} into Kafka topic")


def json_serialize(data) : 
    return json.dumps(data).encode('utf-8')


def extract_date(date_string : str ) : 
    match = re.match(r'(\d{4})_(\d{2})_(\d{2})', date_string)

    if match:
        year, month, date = match.groups()
        return int(year),int(month),int(date)
    else: return None,None,None


def get_file_1day(directory : str , cur_file_number : int) : 
    all_files_day = []
    all_files = os.listdir(directory)
    if cur_file_number >= len(all_files) : return None,-1
    cur_file = all_files[cur_file_number]
    all_files_day.append(cur_file)
    cur_year, cur_month, cur_date = extract_date(cur_file)

    current_date = datetime.datetime(cur_year,cur_month,cur_date)
    cur_file_number +=1
    while True : 
        next_file = all_files[cur_file_number]
        next_year, next_month, next_date = extract_date(next_file)
        next_date = datetime.datetime(next_year,next_month,next_date)
        if (next_date - current_date).days == 0 : 
            all_files_day.append(next_file)
            cur_file_number +=1
        else:
            break

    return all_files_day,cur_file_number


def read_file_offset(file_name : str) : 
    with open(file_name, 'r') as file:
        num = int(file.read())
    return num

def write_file(file_name : str, file_num : int) : 
    with open(file_name, 'w') as file:
        file.write(f"{file_num}")

def stream_1_day(directory : str , day_files : list,producer : KafkaProducer) : 
    for file in day_files : 
        print(file)
        insert_data_into_kafka(directory,file,producer)

        
    print("Finish streaming data of 1 day")



if __name__ == "__main__"  :
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer = json_serialize)

    green_file_name = './offset_data/green_file_offset.txt'
    
    green_directory ='./../raw_data/green_data'

    current_greenfile_offset = read_file_offset(green_file_name)
    greenday_files,next_greenfile_num = get_file_1day(green_directory,current_greenfile_offset)


    
    if greenday_files is None or next_greenfile_num == -1 : 
        print("There is no new data for green taxi")
    else:
        time.sleep(1)
        stream_1_day(green_directory,greenday_files,producer)


        write_file(green_file_name,next_greenfile_num)