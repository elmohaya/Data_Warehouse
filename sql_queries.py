import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
artist VARCHAR,
auth VARCHAR,
firstname VARCHAR,
gender VARCHAR,
itemInSession INT,
lastname VARCHAR,
length FLOAT,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration FLOAT,
sessionId INT,
song VARCHAR,
status INT,
ts TIMESTAMP,
userAgent VARCHAR,
userId INT
)

""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
num_songs INT,
artist_id VARCHAR,
artist_latitude FLOAT,
artist_longitude FLOAT,
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR,
title VARCHAR,
duration FLOAT,
year INT
    )
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXIST songplays(
songplay_id INT IDENTITY(0,1),
start_time TIME NOT NULL,
user_id VARCHAR NOT NULL,
level VARCHAR,
song_id VARCHAR NOT NULL,
artist_id VARCHAR NOT NULL,
session_id VARCHAR NOT NULL,
location VARCHAR,
user_agent TEXT,
PRIMARY KEY (songplay_id))
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
user_id INT NOT NULL,
first_name TEXT,
last_name TEXT,
gender TEXT,
level VARCHAR,
PRIMARY KEY (user_id))
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
song_id INT NOT NULL,
title VARCHAR,
artist_id VARCHAR,
year INTEGER,
duration FLOAT,
PRIMARY KEY (song_id))
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
artist_id VARCHAR NOT NULL,
name VARCHAR,
location VARCHAR,
latitude FLOAT,
longitude FLOAT,
PRIMARY KEY (artist_id))
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
start_time TIMESTAMP NOT NULL,
hour INT NOT NULL,
day INTEGER NOT NULL,
week INTEGER NOT NULL,
month INTEGER NOT NULL,
year INTEGER NOT NULL,
weekday VARCHAR NOT NULL,
PRIMARY KEY (start_time))
""")

# STAGING TABLES

staging_events_copy = (""" copy staging_events from {data_bucket} credentials 'aws_iam_role={role_arn}' region 'us-west-2' format as JSON {log_json_path} timeformat as 'epochmillisecs'; """).format(data_bucket=config['S3']['LOG_DATA'], role_arn=config['IAM_ROLE']['ARN'], log_json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = (""" copy staging_songs from {data_bucket} credentials 'aws_iam_role={role_arn}' region 'us-west-2' format as JSON 'auto'; """).format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

SELECT DISTINCT 
s1.ts as start_time,
s1.userId as user_id,
s1.level as level,
s2.song_id as song_id,
s2.artist_id as artist_id,
s1.sessionId as session_id,
s1.location as location,
s1.userAgent as userAgent,

FROM staging_events s1
JOIN staging_songs s2
ON s1.artist=s2.artist_name

WHERE s1.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users(first_name, last_name, gender, level)

SELECT DISTINCT
s1.userId as user_id
s1.firstName as first_name,
s1.lastName as last_name,
s1.gender as gender,
s1.level as level,

FROM staging_events s1

WHERE s1.page = 'NextSong';

""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)

SELECT DISTINCT
s2.song_id as song_id,
s2.title as title,
s2.artist_id as artist_id,
s2.year as year,
s2.duration as duration,

FROM staging_songs s2
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, lattitude, longitude)

SELECT DISTINCT 
s2.artist_id as artist_id,
s2.artist_name as name,
s2.artist_location as location,
s2.artist_lattitude as lattitude,
s2.artist_lngitude as longitude,

FROM staging_songs s2
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)

SELECT DISTINCT
sp.start_time as start_time,
EXTRACT(hour FROM start_time) as hour,
EXTRACT(day FROM start_time) as day,
EXTRACT(week FROM start_time) as week,
EXTRACT(month FROM start_time) as month,
EXTRACT(year FROM start_time) as year,
EXTRACT(weekday FROM start_time) as weekday,

FROM songplays sp
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
