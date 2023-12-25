
def process_data(batchDF, batchId):
    from pyspark.sql.functions import col, struct, expr, from_json
    from pyspark.sql.types import StringType, StructType, IntegerType, StructField
    from time import time
    from pyspark.sql.functions import lit

    kafka_bootstrap_servers = "localhost:9092"
    input_kafka_topic = "spotify-raw"
    output_kafka_topic = "spotify-record"
    checkpoint_dir = "file:///home/hooniegit/git/Spotify-DemoProject/spark/checkpoint" 
    
    start_time = time()
    
    transformed_df = batchDF \
                    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                    
    try:
        transformed_df_2 = transformed_df \
            .withColumn("json_data", from_json(col("value"), StructType([
                StructField("data", IntegerType(), True),
                StructField("column_2", IntegerType(), True),
            ])))\
            .select("key", "json_data.*")
        try:
            transformed_df_2 \
                .write \
                .mode('append') \
                .parquet("file:///home/hooniegit/git/Spotify-DemoProject/spark/data/parquet")
        except Exception as e:
            print(f">>>>>>>>>>>>ERROR : {e}")        
        
    except Exception as e:
        print(f">>>>>>>>>>>>ERROR : {e}")
        transformed_df_2 = transformed_df
      
    end_time = time()
    spent_time = str({"time_spent": float(f"{end_time - start_time:.2f}")})
    time_df = batchDF.drop("value") \
                     .withColumn("value", lit(spent_time))

    time_df \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", output_kafka_topic) \
        .option("checkpointLocation", f"{checkpoint_dir}/query2_checkpoint")
        
# 23-12-25:{'data':12345, 'column_2':88888}
