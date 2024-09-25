import logging
import uuid

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

def create_keyspace(session):
    session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streaming
            WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};                   
                    """)
    
    print("Keyspace created successfully")

def create_table(session):
    session.execute("""
                    CREATE TABLE IF NOT EXISTS spark_streaming.created_users(
                    id TEXT PRIMARY KEY,
                    first_name TEXT,
                    last_name TEXT,
                    gender TEXT,
                    address TEXT,
                    post_code TEXT,
                    email TEXT,
                    username TEXT,
                    registered_date TEXT,
                    phone TEXT,
                    picture TEXT
                    );
                    """)
    print("Table created successfully")


def insert_data(session, **kwargs):
    print("inserting data...")

    id = kwargs.get('id', uuid.uuid4())
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    post_code = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try: 
        session.execute("""INSERT INTO spark_streaming.created_users (id, first_name, last_name, gender, address, post_code, email, username, registered_date, phone, picture) VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""", 
                    (id, first_name, last_name, gender, address, post_code, email, username, registered_date, phone, picture))
        logging.info(f"Data for user: {last_name} {first_name} inserted successfully")

    except Exception as e:
        logging.error(f"Couldn't insert data due to error: {e}")
        


def create_spark_connection():
    s_conn = None 
    try:
        s_conn = SparkSession.builder \
                    .appName('SparkDataStreaming') \
                    .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
                            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
                    .config('spark.cassandra.connection.host', 'localhost') \
                    .getOrCreate()
        
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")

    except Exception as e:
        logging.error(f"Couldn't create Spark session due to error: {e}")

    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None

    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info(f"Streaming Dataframe created successfully")
        

    except Exception as e:
        logging.error(f"Couldn't create Streaming Dataframe due to error: {e}")

  
    return spark_df

def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        return cas_session

    except Exception as e:
        logging.error(f"Couldn't create Cassandra connection due to error: {e}")
        return None
    
def create_selection_from_kafka(spark_df):

    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST (value AS STRING)") \
                  .select(from_json(col('value'), schema).alias("data")).select("data.*")
    print(sel)
    '''
    query = sel.writeStream \
               .outputMode("append") \
               .format("console") \
               .start()

    query.awaitTermination()
    '''
    return sel

if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        selected_df = create_selection_from_kafka(spark_df) 
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is starting...")
            streaming_query = (selected_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streaming')
                               .option('table', 'created_users')
                               .start())
            
            streaming_query.awaitTermination()



