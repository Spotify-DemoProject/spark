import os, sys

lib_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../lib")
sys.path.append(lib_dir)

from demo_parquet import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("streaming_pub_sub") \
                  .setMaster("spark://workspace:7077")
sparkContext = SparkContext(conf=conf)
spark = SparkSession(sparkContext=sparkContext)

kafka_bootstrap_servers = "localhost:9092"
input_kafka_topic = "spotify-raw"
output_kafka_topic = "spotify-record"

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_kafka_topic) \
    .load()

query = df \
    .writeStream \
    .foreachBatch(process_data) \
    .start()

query.awaitTermination()
