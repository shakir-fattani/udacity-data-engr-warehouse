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

staging_events_table_create= ("""CREATE TABLE staging_events(
    event_id INTEGER IDENTITY(0,1),
    artist VARCHAR(255),
    auth VARCHAR(50),
    user_first_name VARCHAR(255),
    user_gender CHAR(1),
    item_in_session	INTEGER,
    user_last_name VARCHAR(255),
    song_length	DOUBLE PRECISION, 
    user_level VARCHAR(10),
    location VARCHAR(255),	
    method VARCHAR(10),
    page VARCHAR(35),	
    registration BIGINT,	
    session_id INTEGER,
    song_title VARCHAR(255),
    status INTEGER, 
    ts BIGINT,
    user_agent TEXT,	
    user_id INTEGER,
    PRIMARY KEY (event_id)
)""")

staging_songs_table_create = ("""CREATE TABLE staging_songs(
    song_id VARCHAR(50),
    title VARCHAR(255),
    artist_id VARCHAR(50),
    artist_name VARCHAR(255),
    artist_location VARCHAR(255),
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    year INTEGER,
    num_songs INTEGER,
    duration DOUBLE PRECISION,
    PRIMARY KEY (song_id)
)""")

songplay_table_create = ("""CREATE TABLE songplays(
    songplay_id INTEGER IDENTITY(0,1),
    start_time TIMESTAMP,
    user_id VARCHAR(50),
    level VARCHAR(50),
    song_id VARCHAR(50),
    artist_id VARCHAR(50),
    session_id INTEGER,
    location VARCHAR(255),
    user_agent TEXT,
    PRIMARY KEY (songplay_id)
)""")

user_table_create = ("""CREATE TABLE users (
    user_id INTEGER,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender CHAR(1),
    PRIMARY KEY (user_id)
)""")

song_table_create = ("""CREATE TABLE songs(
    song_id VARCHAR(50),
    title VARCHAR(255),
    artist_id VARCHAR(50) NOT NULL,
    year INTEGER,
    duration DOUBLE PRECISION,
    PRIMARY KEY (song_id)
)""")

artist_table_create = ("""CREATE TABLE artists(
    artist_id VARCHAR(50),
    name VARCHAR(255),
    location VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    PRIMARY KEY (artist_id)
)""")

time_table_create = ("""CREATE TABLE time (
    start_time TIMESTAMP,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    week_day INTEGER,
    is_weekend BOOLEAN,
    PRIMARY KEY (start_time)
)""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events 
                          from {}
                          iam_role {}
                          json {};""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs 
                          from {} 
                          iam_role {}
                          json 'auto';""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT  
    to_timestamp(e.ts/1000) as start_time, e.user_id, e.user_level, s.song_id,
    s.artist_id, e.session_id, e.location, e.user_agent
FROM staging_events e, staging_songs s
WHERE e.page = 'NextSong' 
AND e.song_title = s.title 
AND e.artist_name = s.artist_name 
AND e.song_length = s.duration""")
# to_timestamp
user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender)
SELECT DISTINCT  
    user_id, 
    user_first_name, 
    user_last_name, 
    user_gender
FROM staging_events""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
SELECT DISTINCT 
    song_id, 
    title,
    artist_id,
    year,
    duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
SELECT DISTINCT artist_id, artist_name, artist_location, 
    artist_latitude, artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, week_day, is_weekend)
SELECT to_timestamp(start_time), 
    extract(hour from start_time), extract(day from start_time),
    extract(week from start_time), extract(month from start_time),
    extract(year from start_time),  extract(dayofweek from start_time),
    CASE WHEN EXTRACT(ISODOW FROM start_time) IN (6, 7) THEN true ELSE false END AS is_weekend
FROM songplays
GROUP BY start_time""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
