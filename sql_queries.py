import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.config')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS times;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar(250),
        auth varchar(50),
        firstName varchar(250),
        gender char(1),
        iteminSession int,
        lastName varchar(250),
        length float,
        level varchar(10),
        location varchar(250),
        method char(3),
        page varchar(20),
        registration bigint,
        sessionId bigint,
        song varchar(250),
        status int,
        ts timestamp,
        userAgent varchar(250),
        userId int
        );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id char(18),
        artist_latitude float,
        artist_location varchar(250),
        artist_longitude float,
        artist_name varchar(250),
        duration float,
        num_songs int,
        song_id char(18),
        title varchar(250),
        year int
        );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id bigint IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp REFERENCES times(start_time) NOT NULL,
        user_id int REFERENCES users(user_id) NOT NULL,
        level varchar(10),
        song_id char(18) REFERENCES songs(song_id),
        artist_id char(18) REFERENCES artists(artist_id),
        session_id bigint,
        location varchar(250),
        user_agent varchar(250))
        DISTSTYLE EVEN;
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id int PRIMARY KEY,
        first_name varchar(250),
        last_name varchar(250),
        gender char(1),
        level varchar(10))
        DISTSTYLE ALL;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id char(18) PRIMARY KEY,
        title varchar(250),
        artist_id char(18),
        year int,
        duration float)
        DISTSTYLE ALL;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id char(18) PRIMARY KEY,
        name varchar(250),
        location varchar(250),
        latitude float,
        longitude float)
        DISTSTYLE ALL;

""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS times (
        start_time timestamp PRIMARY KEY,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int)
        DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = (f"""
    COPY staging_events FROM {config['S3']['LOG_DATA']}
        CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}'
        REGION 'us-west-2'
        FORMAT as JSON {config['S3']['LOG_JSONPATH']}
        TIMEFORMAT as 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""")

staging_songs_copy = (f"""
    COPY staging_songs FROM {config['S3']['SONG_DATA']}
        CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}'
        REGION 'us-west-2'
        FORMAT as JSON 'auto'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""")

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        (SELECT e.ts, e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent
        FROM staging_events e
        LEFT JOIN staging_songs s ON s.title = e.song AND s.artist_name = e.artist AND abs(s.duration - e.length) < 5
        WHERE e.page = 'NextSong');
""")

user_table_insert = ("""
    INSERT INTO users
        (SELECT userId, firstName, lastName, gender, level FROM (
            SELECT 
                userId, firstName, lastName, gender, level,
                ROW_NUMBER() OVER (PARTITION BY userId ORDER BY ts DESC) AS userId_row
            FROM staging_events
            WHERE page = 'NextSong'
        ) AS u
        WHERE userId_row = 1);
""")

song_table_insert = ("""
    INSERT INTO songs
        (SELECT song_id, title, artist_id, year, duration
        FROM staging_songs);
""")

artist_table_insert = ("""
    INSERT INTO artists
        (SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM (
            SELECT 
                artist_id, artist_name, artist_location, artist_latitude, artist_longitude,
                ROW_NUMBER() OVER (PARTITION BY artist_id) AS artist_id_row
            FROM staging_songs
        ) AS a
        WHERE artist_id_row = 1);
""")

time_table_insert = ("""
    INSERT INTO times
        (SELECT ts, ts_hour, ts_day, ts_week, ts_month, ts_year, ts_dow FROM (
            SELECT 
                ts, DATE_PART('hour', ts) as ts_hour, DATE_PART('day', ts) as ts_day, DATE_PART('week', ts) as ts_week, DATE_PART('month', ts) as ts_month, DATE_PART('year', ts) as ts_year, DATE_PART('dow', ts) as ts_dow,
                ROW_NUMBER() OVER (PARTITION BY ts) AS ts_row
            FROM staging_events
            WHERE page = 'NextSong'
            ) AS t
        WHERE ts_row = 1);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
