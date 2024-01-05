
def create_artists(spark, json_dir:str):
    from pyspark.sql.functions import explode
    
    df = spark.read.option("multiline", "true").json(json_dir) \
        .withColumn("artists", explode("artists")) \
        .withColumn("images", explode("artists.images")) \
        .filter("images.height == 640") \
        .selectExpr(
            "artists.id",
            "artists.name",
            "artists.popularity",
            "artists.followers.total as followers",
            "images.url as image_url",
            "explode(artists.genres) as genre"
        )
    
    return df

def create_albums(spark, json_dir:str):
    from pyspark.sql.functions import explode
    
    df = spark.read.option("multiline", "true").json(json_dir) \
        .withColumn("albums", explode("albums")) \
        .withColumn("images", explode("albums.images")) \
        .filter("images.height = 640") \
        .withColumn("tracks", explode("albums.tracks.items")) \
        .withColumn("artists", explode("albums.artists")) \
        .selectExpr(
            "albums.id",
            "albums.name",
            "images.url as image_url",
            "artists.id as artist_id",
            "tracks.id as track_id",
            "tracks.name as track_name",
            "tracks.duration_ms as duration_ms",
            "tracks.explicit as explicit",
            "tracks.disc_number as disc_number",
            "tracks.track_number as track_number",
        )
    
    return df

def create_tracks(spark, json_dir:str):
    from pyspark.sql.functions import explode
    
    df = spark.read.option("multiline", "true").json(json_dir) \
    .withColumn("tracks", explode("tracks")) \
    .selectExpr("tracks.id",
                "tracks.popularity")
    
    return df

def create_tracks_af(spark, json_dir:str):
    from pyspark.sql.functions import explode

    df = spark.read.option("multiline", "true").json(json_dir) \
        .withColumn("audio_features", explode("audio_features")) \
        .selectExpr("audio_features.id",
                    "audio_features.key",
                    "audio_features.mode",
                    "audio_features.time_signature",
                    "audio_features.tempo",
                    "audio_features.acousticness",
                    "audio_features.danceability",
                    "audio_features.energy",
                    "audio_features.instrumentalness",
                    "audio_features.liveness",
                    "audio_features.loudness",
                    "audio_features.speechiness",
                    "audio_features.valence")
    
    return df
