from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as sf

def preprocess(initial_df) : 
    initial_df = initial_df.drop(*['trip_id','ehail_fee'])
    preprocess_df = initial_df.withColumn('payment_type',sf.col('payment_type').cast(IntegerType()))
    preprocess_df = preprocess_df.withColumn('ratecodeid',sf.col('ratecodeid').cast(IntegerType()))
    preprocess_df = preprocess_df.withColumn('trip_type',sf.col('trip_type').cast(IntegerType()))
    preprocess_df = preprocess_df.withColumn('vendorid',sf.col('vendorid').cast(IntegerType()))
    preprocess_df = preprocess_df.withColumn('lpep_pickup_datetime',sf.to_timestamp(sf.col('lpep_pickup_datetime')))
    preprocess_df = preprocess_df.withColumn('lpep_dropoff_datetime',sf.to_timestamp(sf.col('lpep_dropoff_datetime')))
    preprocess_df = preprocess_df.withColumn('lpep_pickup_datetime', sf.date_format(sf.col('lpep_pickup_datetime'), "yyyy-MM-dd HH:mm:ss"))
    preprocess_df = preprocess_df.withColumn('lpep_dropoff_datetime', sf.date_format(sf.col('lpep_dropoff_datetime'), "yyyy-MM-dd HH:mm:ss"))
    return preprocess_df


def read_info(spark,db,table_name) : 
    url = f"jdbc:sqlserver://host.docker.internal:1433;databaseName={db};encrypt=false;trustServerCertificate=true"
    properties = {
        "user": "sa",
        "password": "tien",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "charset": "UTF-8" 
    }
    info_df = spark.read.jdbc(url=url, table=table_name, properties=properties)
    return info_df


def ETL_green(preprocess_df,flag_df,date_df) : 
    df = preprocess_df.withColumn('Year',sf.year(sf.col('lpep_dropoff_datetime')))
    df = df.withColumn('Month',sf.month(sf.col('lpep_dropoff_datetime')))
    df = df.withColumn('Day',sf.dayofmonth(sf.col('lpep_dropoff_datetime')))
    df = df.withColumn('Hour',sf.hour(sf.col('lpep_dropoff_datetime')))
    new_df = df.join(date_df , on=['Year','Month','Day','Hour'],how='inner')
    new_df = new_df.withColumnRenamed('DateTimeID','PickupDateID')
    new_df = new_df.drop(*['Year','Month','Day','Hour','DateTime','DayOfWeek','MonthName','DayName'])


    new_df = new_df.withColumn('Year',sf.year(sf.col('lpep_dropoff_datetime')))
    new_df = new_df.withColumn('Month',sf.month(sf.col('lpep_dropoff_datetime')))
    new_df = new_df.withColumn('Day',sf.dayofmonth(sf.col('lpep_dropoff_datetime')))
    new_df = new_df.withColumn('Hour',sf.hour(sf.col('lpep_dropoff_datetime')))
    new_df = new_df.join(date_df , on=['Year','Month','Day','Hour'],how='inner')
    new_df = new_df.withColumnRenamed('DateTimeID','DropoffDateID')
    new_df = new_df.drop(*['Year','Month','Day','Hour','DateTime','DayOfWeek','MonthName','DayName'])


    new_df = new_df.join(flag_df,new_df['store_and_fwd_flag'] == flag_df['Flag_Code'],how='inner')
    new_df = new_df.drop(*['Flag_Code','Flag','store_and_fwd_flag'])

    new_df = new_df.withColumn('service_type',sf.lit('green'))
    new_df = new_df.withColumn('airport_fee',sf.lit(0.0))
    new_df = new_df.withColumnsRenamed({'lpep_pickup_datetime' : 'pickup_datetime','lpep_dropoff_datetime' : 'dropoff_datetime','payment_type' : 'PaymentTypeID','trip_type':'TripTypeID'})

    return new_df

def write_data(final_df,table_name) : 
    url = "jdbc:sqlserver://host.docker.internal:1433;databaseName=nyc_taxi;encrypt=false;trustServerCertificate=true"
    properties = {
        "user": "sa",
        "password": "tien",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    try:
        final_df.write.jdbc(url=url, table=table_name, mode="append", properties=properties)
        print(f"Successfully inserted data into table {table_name}")

    except Exception as e:
        print(f"Error inserting data into table {table_name}: {str(e)}")  


    return print(f"Writing data to {table_name} successfully")



def get_latest_raw_db(spark , table,keyspace ) :
    data = spark.read.format('org.apache.spark.sql.cassandra').options(table=table,keyspace=keyspace).load()
    latest_time_row = data.select('lpep_pickup_datetime').agg({'lpep_pickup_datetime':'max'}).collect()

    latest_time = latest_time_row[0][0] if latest_time_row else None
    
    if latest_time is None : 
        return '1900-01-01 00:00:00'
    else : return latest_time




def get_latest_time_dw(spark) : 
    url = "jdbc:sqlserver://host.docker.internal:1433;databaseName=nyc_taxi;encrypt=false;trustServerCertificate=true"
    properties = {
        "user": "sa",
        "password": "tien",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "charset": "UTF-8" 
    }
    info_df = spark.read.jdbc(url=url, table='fact_trips', properties=properties)
    info_df = info_df.filter(sf.col('service_type') == 'green')
    latest_time_row = info_df.select('pickup_datetime').agg({'pickup_datetime':'max'}).collect()
    latest_time = latest_time_row[0][0] if latest_time_row else None
    if latest_time is None : 
        return '1900-01-01 00:00:00'
    else : return latest_time.strftime('%Y-%m-%d %H:%M:%S')


if __name__ == '__main__' : 
    
    spark = SparkSession.builder \
            .config("spark.cassandra.connection.host", "my-cassandra") \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.cassandra.auth.username", "cassandra") \
            .config("spark.cassandra.auth.password", "cassandra") \
            .getOrCreate()
    latest_raw_time = get_latest_raw_db(spark,'green_taxi','nyc_taxi')
    print("Latest time in raw data in Cassandra : ",latest_raw_time)
    latest_preprocess_time = get_latest_time_dw(spark)
    print("Latest time in data warehouse SQL Server : ",latest_preprocess_time)
    if latest_raw_time > latest_preprocess_time :
        print("-------------------------------------------------------------------------------------------------------------------")
        print("Extract data ")
        initial_df = spark.read.format('org.apache.spark.sql.cassandra').options(table='green_taxi',keyspace='nyc_taxi').load()
        initial_df.show()

        print("-------------------------------------------------------------------------------------------------------------------")
        print("Filter the latest data ")
        initial_df = initial_df.filter(sf.col('lpep_pickup_datetime') > latest_preprocess_time)
        initial_df = initial_df.filter(sf.col('RatecodeID') <= 6)
        print("-------------------------------------------------------------------------------------------------------------------")
        print("Extract the dimension table ")
        date_df = read_info(spark,'nyc_taxi','Dim_date')
        flag_df = read_info(spark,'nyc_taxi','Dim_flag')

        print("-------------------------------------------------------------------------------------------------------------------")
        print("Preprocessing data ")
        preprocess_df = preprocess(initial_df)
        preprocess_df.printSchema()

        print("-------------------------------------------------------------------------------------------------------------------")
        print("Process data ")
        final_df = ETL_green(preprocess_df,flag_df,date_df)
        final_df.show()
        print("-------------------------------------------------------------------------------------------------------------------")
        print("Writing data ")
        write_data(final_df,'fact_trips')
    else : 
        print("There is no new data")

    spark.stop()