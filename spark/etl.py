from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc
from dotenv import load_dotenv
import os
import duckdb

def main():
    load_dotenv(override=True)

    spark = SparkSession.builder.appName('Subset of Million Song Dataset ETL')\
            .config('spark.jars', os.getenv('JARS_PATH'))\
            .config('spark.executor.extraClassPath', os.getenv('EXECUTOR_PATH'))\
            .config('spark.driver.extraClassPath', os.getenv('DRIVER_PATH'))\
            .getOrCreate()
    
    db_properties = {
        'user': os.getenv('USERNAME'),
        'password': os.getenv('PASSWORD'),
        'driver': 'org.postgresql.Driver'
    }

    """
    Extract
    """
    staging_songs_df = spark.read.jdbc(url=os.getenv('JDBC_URL') ,table='staging_songs', properties=db_properties)
   
    """
    Transform
    """
    # drop duplicates rows
    staging_songs_df = staging_songs_df.dropDuplicates()

    # handle null values
    staging_songs_df =  staging_songs_df.na.fill({'artist_latitude': 0, 'artist_longtitude': 0, 'artist_location': 'unknown', 'mb_id': '0'})

    # drop coloumn
    staging_songs_df = staging_songs_df.drop(col('danceability')).drop(col('mode')).drop(col('mode_confidence'))\
                                       .drop(col('key')).drop(col('key_confidence')).drop(col('energy'))\
                                       .drop(col('time_signature')).drop(col('time_signature_confidence'))\
                                       .drop(col('end_of_fade_in')).drop(col('start_of_fade_out')).drop(col('mb_id'))\
                                       .drop(col('key_signature')).drop(col('key_signature_confidence'))
    
    
    # split data
    songs_df = staging_songs_df.select('song_id', 'title', 'year', 'duration')
    artists_df = staging_songs_df.select('artist_id', 'artist_name', 'artist_latitude', 'artist_longtitude', 'artist_location', 'artist_familiarity')
    albums_df = staging_songs_df.select('album_id', 'album_name')
    fact_songs_df = staging_songs_df.select(
            'song_id', 'artist_id', 'album_id', 'hotness',  'tempo', 'loudness' 
    )
        
    # rename column
    songs_df = songs_df.withColumnRenamed('song_id', 'id')
    artists_df = artists_df.withColumnsRenamed({'artist_id': 'id', 'artist_name': 'name', 'artist_latitude': 'latitude', 'artist_longtitude': 'longtitude', 'artist_location': 'location', 'artist_familiarity': 'familiarity'})
    albums_df = albums_df.withColumnsRenamed({'album_id': 'id', 'album_name': 'name'})

    # sort by name, title
    songs_df = songs_df.sort(asc('title'))
    artists_df = artists_df.sort(asc('name'))
    albums_df = albums_df.sort(asc('name'))

    # convert to pandas
    fact_songs_df_pandas = fact_songs_df.toPandas()
    songs_df_pandas = songs_df.toPandas()
    artists_df_pandas = artists_df.toPandas()
    albums_df_pandas = albums_df.toPandas()

    """
    Load
    """
    con = duckdb.connect('datasongs.duckdb')

    # save pandas dataframe to duckdb
    con.execute("CREATE TABLE IF NOT EXISTS songs AS SELECT * FROM songs_df_pandas")
    con.execute("CREATE TABLE IF NOT EXISTS  artists AS SELECT * FROM artists_df_pandas")
    con.execute("CREATE TABLE IF NOT EXISTS  albums AS SELECT * FROM albums_df_pandas")
    con.execute("CREATE TABLE IF NOT EXISTS  fact_songs AS SELECT * FROM fact_songs_df_pandas")

    con.close()
    spark.stop()


if __name__ == "__main__":
    main()