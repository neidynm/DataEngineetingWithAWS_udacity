import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")
DWH_ROLE_ARN  = config.get("IAM_ROLE","ARN")
DHW_ROLE_NAME = config.get("IAM_ROLE","ROLE_NAME")


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS list_of_events_stagging; "
staging_songs_table_drop =  "DROP TABLE IF EXISTS songs_stagging; "
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays; "
user_table_drop =  "DROP TABLE IF EXISTS dim_users; "
song_table_drop =  "DROP TABLE IF EXISTS dim_song; "
artist_table_drop =  "DROP TABLE IF EXISTS dim_artist; "
time_table_drop =  "DROP TABLE IF EXISTS dim_time; "

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS list_of_events_stagging(
        artist TEXT,
        auth TEXT NOT NULL,
        firstName TEXT,
        gender CHAR(1),
        itemInSession NUMERIC,
        lastName TEXT,
        length DOUBLE PRECISION,
        level TEXT NOT NULL,
        location TEXT,
        method VARCHAR(15) NOT NULL,
        page TEXT NOT NULL,
        registration DOUBLE PRECISION,
        session INTEGER,
        song TEXT,
        status NUMERIC NOT NULL,
        ts TIMESTAMP,
        userAgent TEXT,
        userId INTEGER
    ) 
    DISTSTYLE AUTO;
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs_stagging (
       num_songs NUMERIC NOT NULL,
       artist_id TEXT  NOT NULL,
       artist_latitude DECIMAL(8,6),
       artist_longitude DECIMAL(9,6),
       artist_location VARCHAR(1024),
       artist_name VARCHAR(1024),
       song_id  VARCHAR NOT NULL,
       title TEXT NOT NULL,
       duration DOUBLE PRECISION NOT NULL,
       year NUMERIC
    )
    DISTSTYLE AUTO;
""")

songplay_table_create = ("""
    CREATE table IF NOT EXISTS fact_songplays (
        songplay_id BIGINT IDENTITY(0,1),
        start_time TIMESTAMP,
        user_id INTEGER,
        level TEXT,
        song_id  VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        session_id INTEGER,
        location TEXT,
        user_agent TEXT
    )
    DISTKEY (song_id)
    SORTKEY (start_time);
""")

user_table_create = ("""
    CREATE table IF NOT EXISTS dim_users(
        user_id INTEGER, 
        first_name TEXT,
        last_name TEXT, 
        gender CHAR(1), 
        level TEXT NOT NULL
    )
    DISTSTYLE ALL;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_song(
        song_id VARCHAR NOT NULL, 
        title TEXT NOT NULL, 
        artist_id VARCHAR NOT NULL, 
        year NUMERIC, 
        duration DOUBLE PRECISION NOT NULL
    )
    DISTKEY (song_id)
    SORTKEY (song_id);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_artist(
        artist_id VARCHAR NOT NULL, 
        name VARCHAR(1024), 
        location VARCHAR(1024), 
        latitude DECIMAL(8,6), 
        longitude DECIMAL(9,6)
    )
    DISTKEY (artist_id)
    SORTKEY (artist_id);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_time(
        start_time TIMESTAMP, 
        hour VARCHAR(6) NOT NULL,
        day INTEGER NOT NULL, 
        week VARCHAR(10) NOT NULL, 
        month VARCHAR(10) NOT NULL,
        year INTEGER NOT NULL,
        weekday VARCHAR(10) NOT NULL
    )
    DISTSTYLE ALL
    SORTKEY (start_time);
""")

# STAGING TABLES
staging_events_copy = ("""
    COPY list_of_events_stagging from {0}
    CREDENTIALS 'aws_iam_role={1}' 
    FORMAT AS JSON 'auto'
    COMPUPDATE OFF REGION 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
""").format(LOG_JSONPATH, DHW_ROLE_NAME)

staging_songs_copy = ("""
    COPY songs_stagging FROM {0}
    CREDENTIALS 'aws_iam_role={1}' 
    FORMAT AS JSON 'auto'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    REGION 'us-west-2';
""").format(SONG_DATA,DHW_ROLE_NAME)

# FINAL TABLES
songplay_table_insert = ("""
     INSERT INTO fact_songplays (start_time, user_id, 
                            level, song_id, artist_id,
                            session_id, location, user_agent)
            SELECT
                me.ts AS start_time, 
                me.userId as user_id,
                me.level as level,
                s.song_id as song_id,
                s.artist_id as artist_id,
                me.session AS session_id,
                me.location,
                me.userAgent as user_agent
            FROM
                 songs_stagging AS s
                 LEFT JOIN list_of_events_stagging AS me ON (s.title = me.song AND me.artist = s.artist_name);
""")

user_table_insert = ("""
    INSERT INTO dim_users ( user_id, first_name, last_name, gender, level)
           SELECT
               me.userId AS user_id,
               me.firstName AS first_name,
               me.lastName AS last_name,
               me.gender AS gender,
               me.level AS level
          FROM list_of_events_stagging AS me;   
""")

song_table_insert = ("""
    INSERT INTO dim_song (song_id, title, artist_id , year, duration)
          SELECT
              s.song_id AS song_id,
              s.title AS title,
              s.artist_id AS artist_id,
              s.year,
              s.duration
          FROM
              songs_stagging AS s
""")

artist_table_insert = ("""
     INSERT INTO dim_artist (artist_id, name, location, latitude, longitude)
        SELECT
            s.artist_id,
            s.artist_name AS name,
            s.artist_location,
            s.artist_latitude AS latitude,
            s.artist_longitude AS longitude
        FROM
           songs_stagging AS s
            
""")

time_table_insert = ("""
    INSERT INTO dim_time ( start_time, hour, day, week, month, year, weekday)
        SELECT 
            m.ts AS start_time,
            EXTRACT(HOUR from m.ts) AS hour,
            EXTRACT(DAY from m.ts) AS day,
            EXTRACT(week from m.ts) AS week,
            EXTRACT(month from m.ts) AS month,
            EXTRACT(year from m.ts) AS year,
            EXTRACT(weekday from m.ts) AS weekday
        FROM
            list_of_events_stagging AS m
""")

# Analysing the DATA
# What is the most played song?
most_played_song = ("""
    SELECT 
        ds.title AS song_title, 
        COUNT(fs.song_id) AS play_count
    FROM 
        fact_songplays fs
    JOIN 
        dim_song ds ON fs.song_id = ds.song_id
    GROUP BY 
        ds.title
    ORDER BY 
        play_count DESC
    LIMIT 1;
""")

# Which artist has the most streams?
most_streams_artist = ("""
    SELECT 
        da.name AS artist_name, 
        COUNT(fs.artist_id) AS stream_count
    FROM 
        fact_songplays fs
    JOIN 
        dim_artist da ON fs.artist_id = da.artist_id
    GROUP BY 
        da.name
    ORDER BY 
        stream_count DESC
    LIMIT 1;
""")


# when we have the most traffic for the top 10 most played artists?
most_traffic_top10_artists = ("""
WITH top_artists AS (
    SELECT
        da.name AS artist_name,
        COUNT(fs.songplay_id) AS play_count
    FROM
        fact_songplays fs
    JOIN
        dim_artist da ON fs.artist_id = da.artist_id
    GROUP BY
        da.name
    ORDER BY
        play_count DESC
    LIMIT 10
)
SELECT
    ta.artist_name,
    dt.hour,
    dt.day,
    dt.week,
    dt.month,
    dt.year,
    COUNT(fs.songplay_id) AS traffic_count
FROM
    top_artists ta
JOIN
    fact_songplays fs ON ta.artist_name = (SELECT da.name FROM dim_artist da WHERE fs.artist_id = da.artist_id)
JOIN
    dim_time dt ON fs.start_time = dt.start_time
GROUP BY
    ta.artist_name, dt.hour, dt.day, dt.week, dt.month, dt.year
ORDER BY
    ta.play_count DESC, traffic_count DESC;
""")
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
