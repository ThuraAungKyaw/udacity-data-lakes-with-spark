import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType as TSType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CREDENTIALS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CREDENTIALS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        - Creates a spark session object to be referenced throughout the process
          Returns the existing object if it has already been created
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Description: This function reads song data from S3, clean and modify them,
                     separate the data into different entities such as song, artist, etc.
                     and then write them back to S3 as partitioned parquet files
        
        Arguments: 
            spark - spark session object to reference and execute spark jobs
            input_data - The Amazon S3 file path to read song data from
            output_data - The file path to which parquet files will be written 
        
        Returns: 
            None
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'artist_name', 'year', 'duration'])
    songs_table = songs_table.dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").parquet(os.path.join(output_data, 'songs'), partitionBy=['year', 'artist_name'])

    # extract columns to create artists table 
    artist_cols = ["artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artist_cols = ["{} as {}".format(col, col.replace("artist_", "")) for col in artist_cols]
    artists_table = df.selectExpr("artist_id", *artist_cols)
    artists_table = artists_table.dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(os.path.join(output_data, 'artists'))



def process_log_data(spark, input_data, output_data):
    """
        Description: This function reads log data from S3, clean and modify them, 
                     separate the data into different entities such as user, song, etc
                     and then write them back to S3 as partitioned parquet files
        
        Arguments: 
            spark - spark session object to reference and execute spark jobs
            input_data - The Amazon S3 file path to read log data from
            output_data - The file path to which parquet files will be written 
        
        Returns: 
            None
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    df = df.withColumn("user_id", col("userId").cast("int"))
    
    # extract columns for users table   
    user_cols = ["user_id", 
                 "firstName as first_name", 
                 "lastName as last_name", 
                 "gender", 
                 "level"]
    
    users_table = df.selectExpr(*user_cols)
    users_table = users_table.dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(os.path.join(output_data, 'users'))
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: ts/ 1000)
    df = df.withColumn('ts', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts).isoformat())
    df = df.withColumn('start_time', get_datetime('ts')) 
    
    # extract columns to create time table
    time_table = df.select('start_time')
    time_table = time_table.withColumn('year', year('start_time'))
    time_table = time_table.withColumn('month', month('start_time'))
    time_table = time_table.withColumn('weekday', dayofweek('start_time'))
    time_table = time_table.withColumn('week', weekofyear('start_time'))
    time_table = time_table.withColumn('day', dayofmonth('start_time'))
    time_table = time_table.withColumn('hour', hour('start_time'))
    time_table = time_table.dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").parquet(os.path.join(output_data, 'time'), partitionBy=['year', 'month'])

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs/")
   
    user_events_df = df.withColumn("songplay_id", monotonically_increasing_id())
    user_events_df = user_events_df.withColumn("session_id", col("sessionId").cast("int"))
    user_events_df = user_events_df.withColumn("user_agent", col("userAgent"))
    user_events_df = user_events_df.withColumn('year', year('start_time'))
    user_events_df = user_events_df.withColumn('month', month('start_time'))
    
    song_df.createOrReplaceTempView('songs')
    user_events_df.createOrReplaceTempView('user_events')
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT ue.songplay_id,
               ue.start_time,
               so.song_id,
               so.artist_id,
               ue.user_id,
               ue.level,
               ue.session_id,
               ue.location,
               ue.user_agent,
               year(start_time) as year,
               month(start_time) as month
        FROM user_events ue
        JOIN songs so
        ON ue.song = so.title AND ue.artist = so.artist_name 
    """)
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").parquet(os.path.join(output_data, 'songplays'), partitionBy=['year', 'month'])


def main():
    """
        - Get or create spark session object 
        - Call functions to process song and log data with necessary parameters
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-thura/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
