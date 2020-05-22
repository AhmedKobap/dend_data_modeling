import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *



def process_song_file(cur, filepath):
    """
    This procedure processes a song file whose filepath has been provided as an arugment.
    It extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the song file
    """
    # open song file
    filepath = filepath
    df = pd.read_json(filepath,lines=True)
    #df.drop(df.index, inplace=True)
    #print(df)
    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].drop_duplicates().values
    #print(song_data)
    cur.executemany(song_table_insert, song_data)

    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].drop_duplicates().values
    cur.executemany(artist_table_insert, artist_data)



def process_log_file(cur, filepath):
    """
        This procedure processes a log file whose filepath has been provided as an arugment.
    It extracts the log information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filepath the file path to the LOG file
    """
    # open log file
    filepath = filepath
    df = pd.read_json(filepath,lines=True)
    #Sdf.drop(df.index, inplace=True)


    # filter by NextSong action
    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'],unit='ms')
    df['tsd'] = pd.DatetimeIndex(df.ts).day
    df['tsw'] = pd.DatetimeIndex(df.ts).week
    df['tsm'] = pd.DatetimeIndex(df.ts).month
    df['tsy'] = pd.DatetimeIndex(df.ts).year
    df['tsdn'] = pd.DatetimeIndex(df.ts).day_name()
    newdf=df[(df.page == "NextSong")]
    # insert time data records
    time_data = newdf[['ts','tsd','tsw','tsm','tsy','tsdn']].drop_duplicates().values
    cur.executemany(time_table_insert, time_data)

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']].drop_duplicates()
    user_df=user_df.dropna()
    user_df=user_df.values
    # insert user records
    cur.executemany(user_table_insert, user_df)

    # insert songplay records
    # get songid and artistid from song and artist tables
    df['userId']=df['userId'].replace(to_replace = '', value = -99) 
    log_data = df[['ts','userId','level','song','artist','sessionId','location','userAgent']].drop_duplicates().values
    cur.executemany(songplay_table_insert, log_data)





def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))
        


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()