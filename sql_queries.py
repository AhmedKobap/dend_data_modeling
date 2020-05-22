# DROP TABLES

songplay_table_drop = "drop table if exists songplay"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"
songplay_tmp_table_drop = "drop table if exists songplays_tmp"
# CREATE TABLES


songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
  (
     songplay_id SERIAL PRIMARY KEY,
     start_time  TIMESTAMP,
     userid      INTEGER,
     level       VARCHAR(20),
     song_id     VARCHAR(25),
     artist_id   VARCHAR(25),
     session_id  INT,
     location    VARCHAR(150),
     user_agent  VARCHAR(750)
  );  
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
  (
     userid     INTEGER NOT NULL PRIMARY KEY,
     first_name VARCHAR(20),
     last_name  VARCHAR(20),
     gender     CHAR(3),
     level      VARCHAR(10)
  );  
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
  (
     song_id   VARCHAR(25) NOT NULL PRIMARY KEY,
     title     VARCHAR(150),
     artist_id VARCHAR(25),
     year      INT,
     duration  FLOAT
  );  
""")

artist_table_create = ("""
 CREATE TABLE IF NOT EXISTS artists
  (
     artist_id VARCHAR(25) NOT NULL PRIMARY KEY,
     name      VARCHAR(150),
     location  VARCHAR(150),
     latitude  FLOAT,
     longitude FLOAT
  );  
""")

time_table_create = ("""
 CREATE TABLE IF NOT EXISTS time
  (
     start_time TIMESTAMP NOT NULL PRIMARY KEY,
     day        INT,
     week       INT,
     month      INT,
     year       INT,
     weekday    VARCHAR(20)
  );  
""")

songplay_tmp_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays_tmp
  (
     start_time TIMESTAMP PRIMARY KEY,
     userid     INTEGER,
     level      VARCHAR(20),
     song_title VARCHAR(450),
     name       VARCHAR(450),
     session_id INT,
     location   VARCHAR(450),
     user_agent VARCHAR(750)
  );  
""")

#Create view for songplay
#songplay_view_create = ("""
#CREATE view if not exists time   (start_time timestamp, day int, week int,month int, year int,weekday varchar(20));
#""")

# INSERT RECORDS

songplay_table_insert = ("""
 TRUNCATE songplays_tmp;
INSERT INTO songplays_tmp
            (
                        start_time,
                        userid,
                        level,
                        song_title,
                        NAME,
                        session_id,
                        location,
                        user_agent
            )
            VALUES
            (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
            );
            INSERT INTO songplays
            (
                        start_time,
                        userid,
                        level,
                        song_id,
                        artist_id,
                        session_id,
                        location,
                        user_agent
            )
SELECT    start_time,
          userid,
          level,
          song_id,
          artist_id,
          session_id,
          a.location,
          user_agent
FROM      songplays_tmp a
LEFT JOIN
          (
                     SELECT     a.artist_id,
                                NAME,
                                title,
                                duration,
                                song_id
                     FROM       artists a
                     INNER JOIN songs b
                     ON         a.artist_id=b.artist_id ) c
ON        a.NAME=c.NAME
AND       a.song_title=c.title;
TRUNCATE songplays_tmp; 
               
""")

#VALUES (%s, %s, %s, %s, %s, %s, %s, %s);

user_table_insert = ("""
 INSERT INTO users
            (
                        userid,
                        first_name,
                        last_name,
                        gender,
                        level
            )
            VALUES
            (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
            )
ON conflict
            (
                        userid
            )
          DO UPDATE
set    first_name=excluded.first_name,
       last_name=excluded.last_name,
       gender=excluded.gender,
       level=excluded.level; 
                 
""")

song_table_insert = ("""
 INSERT INTO songs
            (
                        song_id,
                        title,
                        artist_id,
                        year,
                        duration
            )
            VALUES
            (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
            )
ON conflict
            (
                        song_id
            )
           DO UPDATE
set    title=excluded.title,
       artist_id=excluded.artist_id,
       year=excluded.year,
       duration=excluded.duration; 
""")

artist_table_insert = ("""
 INSERT INTO artists
            (
                        artist_id,
                        NAME,
                        location,
                        latitude,
                        longitude
            )
            VALUES
            (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
            )
ON conflict
            (
                        artist_id
            )
           DO UPDATE
set    NAME=excluded.NAME,
       location=excluded.location,
       latitude=excluded.latitude,
       longitude=excluded.longitude; 
""")


time_table_insert = ("""
 INSERT INTO time
            (
                        start_time,
                        day,
                        week,
                        month,
                        year,
                        weekday
            )
            VALUES
            (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
            )
ON conflict
            (
                        start_time
            )
            do nothing; 
""")

#title, artist name, and duration of a song.


# FIND SONGS

#song_select = (""" SELECT s.song_id, a.artist_id from (songs s INNER JOIN artists a ON a.artist_id=s.artist_id) WHERE s.title = %s and a.name = %s and s.duration = %s
#""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,songplay_tmp_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop,songplay_tmp_table_drop]