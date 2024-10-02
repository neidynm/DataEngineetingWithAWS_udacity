DROP_ALL_TABLES = """
    DROP TABLE IF EXISTS staging_events;
    DROP TABLE IF EXISTS staging_songs;
    DROP TABLE IF EXISTS songplays;
    DROP TABLE IF EXISTS users;
    DROP TABLE IF EXISTS songs;
    DROP TABLE IF EXISTS artists;
    DROP TABLE IF EXISTS time;
"""

CREATE_STAGING_EVENTS = """
    CREATE TABLE IF NOT EXISTS staging_events (
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              CHAR(1),
        itemInSession       INT,
        lastName            VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            TEXT,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        sessionId           INT,
        song                VARCHAR,
        status              INT,
        ts                  BIGINT,
        userAgent           TEXT,
        userId              VARCHAR
); """
CREATE_STAGING_SONGS = """ CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs           INT,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     TEXT,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INT
); """
CREATE_SONGS_PLAY = """
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id         VARCHAR     PRIMARY KEY,
        start_time          TIMESTAMP   NOT NULL,
        userid              VARCHAR,
        level               VARCHAR,
        song_id             VARCHAR,
        artist_id           VARCHAR,
        sessionid           INT,
        location            TEXT,
        useragent           TEXT
); """

CREATE_DIM_USERS = """ CREATE TABLE IF NOT EXISTS dim_users(
    userid              VARCHAR     PRIMARY KEY,
    first_name          VARCHAR,
    last_name           VARCHAR,
    gender              CHAR(1),
    level               VARCHAR
);"""

CREATE_DIM_SONGS = """  CREATE TABLE IF NOT EXISTS dim_songs (
    song_id             VARCHAR     PRIMARY KEY,
    title               VARCHAR,
    artist_id           VARCHAR,
    year                INT,
    duration            FLOAT
);"""

CREATE_DIM_ARTISTS = """ CREATE TABLE IF NOT EXISTS dim_artists (
    artist_id           VARCHAR     PRIMARY KEY,
    name                VARCHAR,
    location            TEXT,
    latitude            FLOAT,
    longitude           FLOAT
);"""

CREATE_DIM_TIME = """ CREATE TABLE IF NOT EXISTS dim_time (
    start_time          BIGINT   PRIMARY KEY,
    hour                INT,
    day                 INT,
    week                INT,
    month               INT,
    year                INT,
    weekday             INT
);"""