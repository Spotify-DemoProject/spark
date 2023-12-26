
def create_artists(spark, json_dir:str):
    from pyspark.sql.functions import col, explode, expr
    
    df = spark.read.option("multiline", "true").json(json_dir) \
    .withColumn("image", explode(expr("images"))) \
    .filter(col("image.height") == 640) \
    .withColumn("artist_id", expr("id")) \
    .withColumn("artist_name", expr("name")) \
    .withColumn("genre", explode(expr("genres"))) \
    .withColumn("followers", expr("followers.total")) \
    .withColumn("artist_image_url", expr("image.url")) \
    .select("artist_id", "artist_name", "genre", "artist_image_url", "followers")
    
    return df

def create_albums(spark, json_dir:str):
    from pyspark.sql.functions import col, explode, expr
    
    df = spark.read.option("multiline", "true").json(json_dir) \
    .withColumn("image", explode(expr("images"))) \
    .filter(col("image.height") == 640) \
    .withColumn("artist_id", explode(expr("artists.id"))) \
    .withColumn("album_id", expr("id")) \
    .withColumn("album_name", expr("name")) \
    .withColumn("album_image_url", expr("image.url")) \
    .withColumn("track", explode(expr("tracks.items"))) \
    .withColumn("track_id", expr("track.id")) \
    .withColumn("disc_number", expr("track.disc_number")) \
    .withColumn("track_number", expr("track.track_number")) \
    .select("album_id", "album_name", "artist_id", "album_type", "label", "album_image_url", "track_id", "track_number", "release_date", "total_tracks")
    
    return df

def create_tracks(spark, json_dir:str):
    from pyspark.sql.functions import explode, expr
    
    df = spark.read.option("multiline", "true").json(json_dir) \
    .withColumn("track_id", expr("id")) \
    .withColumn("track_name", expr("name")) \
    .withColumn("artist_id", explode(expr("artists.id"))) \
    .select("track_id", "track_name", "artist_id", "duration_ms", "explicit")
    
    return df
