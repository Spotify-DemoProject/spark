
def create_artists(spark, json_dir:str):
    from pyspark.sql.functions import col, explode, expr
    
    df = spark.read.option("multiline", "true").json(json_dir) \
    .select(explode('artists').alias('artists')) \
    .withColumn("image", explode(expr("artists.images"))) \
    .filter(col("image.height") == 640) \
    .withColumn("artist_id", expr("artists.id")) \
    .withColumn("artist_name", expr("artists.name")) \
    .withColumn("genre", explode(expr("artists.genres"))) \
    .withColumn("followers", expr("artists.followers.total")) \
    .withColumn("artist_image_url", expr("image.url")) \
    .select("artist_id", "artist_name", "genre", "artist_image_url", "followers")
    
    return df

def create_albums(spark, json_dir:str):
    from pyspark.sql.functions import col, explode, expr
    
    df = spark.read.option("multiline", "true").json(json_dir) \
        .select(explode('albums').alias('albums')) \
        .withColumn("image", explode(expr("albums.images"))) \
        .filter(col("image.height") == 640) \
        .withColumn("artist_id", explode(expr("albums.artists.id"))) \
        .withColumn("album_id", expr("albums.id")) \
        .withColumn("album_name", expr("albums.name")) \
        .withColumn("album_image_url", expr("image.url")) \
        .withColumn("track", explode(expr("albums.tracks.items"))) \
        .withColumn("track_id", expr("track.id")) \
        .withColumn("track_name", expr("track.name")) \
        .withColumn("duration_ms", expr("track.duration_ms")) \
        .withColumn("explicit", expr("track.explicit")) \
        .withColumn("disc_number", expr("track.disc_number")) \
        .withColumn("track_number", expr("track.track_number")) \
        .select("album_id", "album_name", "artist_id", "albums.album_type", "albums.label", "album_image_url", "track_id", "track_name", "track_number", "disc_number", "duration_ms", "explicit", "albums.release_date", "albums.total_tracks")

    
    return df

