import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

# 1. Create connection to cassandra
# 2. Create connection to Apache Spark
# 3. Create the keyspace
# 4. Create the database table
# 5. Insert the data streaming from kafka to the database table
# create a keyspace for cassandra
def create_keyspace(session):
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS
    WITH replication = {'class': 'SimpleStrategy, 'replication_factor': '1'};
    """)

    print("Keyspace created successfully")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.users (
        user_id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        city TEXT,
        email TEXT,
        username TEXT,
        profile_picture TEXT,
        password_salt TEXT,
        password_hash TEXT)""")
    
    print("Table created successfully")


def insert_data(session, **kwargs):
    print("Inserting data into database table")

    user_id = kwargs.get("id")
    first_name = kwargs.get("first_name")
    last_name = kwargs.get("last_name")
    gender = kwargs.get("gender")
    city = kwargs.get("city")
    email = kwargs.get("email")
    username = kwargs.get("username")
    profile_picture = kwargs.get("profile_picture")
    password_salt = kwargs.get("password_salt")
    password_hash = kwargs.get("password_hash")

    try:
        session.execute("""
        INSERT INTO spark_streams.users(user_id, first_name, last_name, gender, email, username, profile_picture, password_salt, password_hash) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)""", (user_id, first_name, last_name, gender, email, username, profile_picture, password_salt, password_hash))
        logging.info(f"Data insertint for {first_name} {last_name}")

def create_connection():
    # connect to spark
    # change localhoost 
    try:
        spark_connection = SparkSession.builder.appName("DataStreaming").config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.13:3.41", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1").config("spark.cassandra.connection.host", "broker").getOrCreate()

        spark_connection.setLogLevel("ERROR")
        logging.info("Connected to Apache Spark successfully.")
    except Exception as e:
        logging.error(f"Spark connection failed: {e}")
    
    return spark_connection


def create_db_connection():
    session = None
    try:
        cluster = Cluster(['localhost'])
        cassandra_session = cluster.connection()

        return cassandra_session
    except Exception as e:
        logging.error(f"Cassandra DB connection failed: {e}")
        return None


# connecting to kafka to extract user data points
def kafka_connection(spark_connection):
    spark_df = None

    try:
        spark_df = spark_connection.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "users_created").option("startingOffsets", "earliest").load()

        logging.info("Spark databframe created successfully")
    except Exception as e:
        logging.warning(f"An error with kafka occurred: {e}")
    
    return spark_df

if __main__ == '__main__':

    spark_connection = create_connection()
    
    if spark_connection is not None:
        data_frame = kafka_connection(spark_connection)
        cassandra_session = create_db_connection()
        if cassandra_session is not None:
            create_keyspace(session)
            create_table(session)
            # insert_data(session)
            # insert data in streams rather than directly
