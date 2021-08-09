import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processing log_data 
        
    Processing song_data from S3 to local directory. 
    Creates song and artists dimension tables"""
    
    # get filepath to song data file
    song_data ="data/song-data/song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, 'songs'),mode="overwrite", partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude','artist_longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists"), mode='overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page='NextSong'")

    # extract columns for users table    
    artists_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()\
                                                                             .withColumnRenamed('userId', 'user_id')\
                                                                             .withColumnRenamed('lastName', 'last_name')\
                                                                             .withColumnRenamed('firstName', 'first_name')
    
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'),mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn('start_time', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
#     get_datetime = udf()
#     df = df.withColumn('start_time', get_timestamp('ts'))
    
    # extract columns to create time table
    time_table = df.select('start_time', hour('start_time').alias('hour'), dayofmonth('start_time').alias('day'),
                      weekofyear('start_time').alias('week'), month('start_time').alias('month'),
                      year('start_time').alias('year'), 
                      date_format(col('start_time'), 'D').cast(IntegerType()).alias('weekday')).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time"), mode="overwrite", partitionBy=[('year' ,'month')])

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, 'song-data/song_data', '*', '*', '*'))

    # extract columns from joined song and log datasets to create songplays table 
    df = df.orderBy('ts')
    df = df.withColumn('songplay_id', monotonically_increasing_id())
    song_df.createOrReplaceTempView('songs')
    df.createOrReplaceTempView('events')
    songplays_table = spark.sql("""
        SELECT
            e.songplay_id,
            e.start_time,
            e.user_id,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId as session_id,
            e.location,
            e.userAgent as user_agent,
            year(e.start_time) as year,
            month(e.start_time) as month
        FROM events e
        LEFT JOIN songs s ON
            e.song = s.title AND
            e.artist = s.artist_name AND
            ABS(e.length - s.duration) < 2
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays'), mode="overwrite",partitionBy=['year', 'month'])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "hdfs:///user/sparkify_data/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
