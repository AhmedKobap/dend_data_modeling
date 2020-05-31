Project Objectives :
The Main goal of this project is to load Log Data of Song Dataset, and events log and processed data into Postgresql Database.
The structure of our data source is jason files, These files divided to two types, first one contains data for songs and artists, second file describes user data and events for chosen songs and artists.  
the database structure is simple star schema contains dimension and fact table.
* dimension table is (song, artist, user, time)
* fact table is (Song_play)
* as shown in uploaded image named Songplay_model.png the fact table is songplay table related with four dimension table (song, artist, user, time).
* Artist table contains general data about artists like artist name and gender.
* Song table contains all songs in the library with data that describes artist, song duration and release date.
* time table is the date and time dimension for the start schema model.
* users table contains all registered users that using the music library.
to load these dimension tables, primary key should be identified for each dimension. according that small analysis happened using sql queries to identify the uniqness for each column.

----------------------------------
during data loding some duplication existed, for dimension data will be updated except time table because time table calculated during runtime.

loading steps:

1- Execute create_tables.py to create the Tables or replace them if existed.

2- Execute etl.py to Load the into postgres database.

implemented ETL do below steps:

1- Reading the Jason Log Files and songs files that existed in Directory, Count them, and prints to the console how many files found for processing.

2- form each jason file identify the required column that we need and loop between all files, then finally load required data to database.

3- if the record existed before in dimension table will be updated except time dimension.
