import os, sys

lib_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../lib")
sys.path.append(lib_dir)

from manufacture_json import *

import pyspark
spark = pyspark.sql.SparkSession.builder.appName("spotify-kafka-streaming") \
            .master("spark://workspace:7077") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()

kafka_bootstrap_servers = "localhost:9092"
input_kafka_topic = "spotify-raw"

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_kafka_topic) \
    .load()

query = df \
    .writeStream \
    .foreachBatch(lambda batchDF, batchId: process_data(batchDF, batchId, spark)) \
    .start()

query.awaitTermination()
