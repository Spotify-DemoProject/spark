import os, sys

lib_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(lib_dir)

from create_dataframe import *

def process_data(batchDF, batchId, spark):
    from pyspark.sql.functions import col, from_json
    from pyspark.sql.types import StringType, StructType, StructField
    from time import time
    from pyspark.sql.functions import lit

    kafka_bootstrap_servers = "localhost:9092"
    output_kafka_topic = "spotify-record"
    checkpoint_dir = "file:///home/hooniegit/git/Spotify-DemoProject/spark/checkpoint" 
    
    start_time = time()
    
    try:
        transformed_df = batchDF \
                    .selectExpr("CAST(value AS STRING)") \
                    .withColumn("json_data", from_json(col("value"), StructType([
                        StructField("insert_date", StringType(), True),
                        StructField("category", StringType(), True),
                    ])))\
                    .select("json_data.*")
        
        insert_date = transformed_df.select("insert_date").first()[0]
        category = transformed_df.select("category").first()[0]
        
        json_dir = f"file:///home/hooniegit/git/Spotify-DemoProject/fastapi/data/{category}/{insert_date}"
        
        if category == "albums":
            df = create_albums(spark=spark, json_dir=json_dir)
        elif category == "artists":
            df = create_artists(spark=spark, json_dir=json_dir)
    
        df \
        .coalesce(1) \
        .write \
        .mode('append') \
        .parquet(f"file:///home/hooniegit/git/Spotify-DemoProject/spark/data/parquet/{category}/{insert_date}")
        

    except Exception as e:
        print(f">>>>>>>>>>>>ERROR : {e}")
      
    end_time = time()
    spent_time = str({"time_spent": float(f"{end_time - start_time:.2f}")})
    
    # TEST <<<<<<<<<
    print(f">>>>>>>>>>> time_spent : {spent_time}")
    
    try:
        time_df = batchDF.drop("value") \
                        .withColumn("value", lit(spent_time))

        time_df \
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("topic", output_kafka_topic) \
            .option("checkpointLocation", f"{checkpoint_dir}/spent_time_checkpoint") \
            .save()

    except Exception as e:
        print(f">>>>>>>>>>>>ERROR : {e}")   

# demo:{'insert_date':'2023-12-25', 'category':'albums'}
# demo:{'insert_date':'2023-12-25', 'category':'artists'}
# demo:{'insert_date':'2023-12-25', 'category':'tracks'}