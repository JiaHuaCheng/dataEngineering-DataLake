"""Provide ETL process in spark."""
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pyspark.sql.functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['default']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['default']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create spark session. This is the first step to interact with spark."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Process all song data to produce analytics table.

    - In this function, we generate columnar table and load back into s3.
        - songs table
        - artists table
    """
    write_mode = "overwrite"

    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/A/A/*.json")
    # define output filepath for each table
    songs_output = os.path.join(output_data, 'songs_par/songs.parquet')
    artists_output = os.path.join(output_data, 'artists_par/artists.parquet')

    # read song data file
    song_df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = song_df[
        'song_id',
        'title',
        'artist_id',
        'year',
        'duration'].dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode(write_mode) \
        .partitionBy("year", "artist_id") \
        .parquet(songs_output)

    # extract columns to create artists table
    artists_table = song_df[
        'artist_id',
        'artist_name',
        'artist_location',
        'artist_latitude',
        'artist_longitude'].dropDuplicates()

    # write artists table to parquet files
    artists_table.write.mode(write_mode).parquet(artists_output)


def process_log_data(spark, input_data, output_data):
    """Process all log data to produce analytics table.

    - In this function, we generate columnar table and load back into s3.
        - users table
        - times table
        - songplays table
    """
    write_mode = "overwrite"
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')
    # define output filepath for each table
    users_output = os.path.join(output_data, 'users_par/users.parquet')
    time_output = os.path.join(output_data, 'time_par/time.parquet')
    songs_output = os.path.join(output_data, 'songs_par/songs.parquet')
    songplays_output = os.path.join(output_data, 'songplays_par/songplays.parquet')
    # read log data file
    log_df = spark.read.json(log_data)

    # filter by actions for song plays
    log_df = log_df.where(log_df.page == 'NextSong')

    # extract columns for users table
    users_table = log_df[
        'userId',
        'firstName',
        'lastName',
        'gender',
        'level'].dropDuplicates()

    # write users table to parquet files
    users_table.write.mode(write_mode).parquet(users_output)

    log_df = log_df.withColumn(
        'start_time',
        (log_df.ts.cast('float')/1000.0).cast("timestamp")
    )

    # extract columns to create time table
    time_table = log_df.select(
        log_df.start_time,
        F.hour("start_time").alias('hour'),
        F.dayofmonth("start_time").alias('day'),
        F.weekofyear("start_time").alias('week'),
        F.month("start_time").alias('month'),
        F.year("start_time").alias('year'),
        F.date_format(F.col("start_time"), "E").alias("weekday")
    ).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.mode(write_mode)\
        .partitionBy("year", "month")\
        .parquet(time_output)

    # read in song data to use for songplays table
    song_df = spark.read.parquet(songs_output)

    # extract columns from joined datasets to create songplays
    songplays_table = log_df.join(song_df, song_df.title == log_df.song)

    songplays_table = songplays_table.select(
        monotonically_increasing_id().alias('songplay_id'),
        'start_time',
        'userId',
        'level',
        'song_id',
        'artist_id',
        'sessionId',
        'location',
        'userAgent'
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.withColumn('year', F.year('start_time'))\
        .withColumn('month', F.month('start_time'))\
        .write.partitionBy('year', 'month').mode(write_mode)\
        .parquet(songplays_output)


def main():
    """Use spark to perform ETL process.

    We load data from s3 buckets and save our result back into s3.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://mypracticedatalakebucket/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
