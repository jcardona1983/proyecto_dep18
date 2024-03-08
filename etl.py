import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

# global varibles used to count the total records processed
song_rows = 0;
log_rows = 0;


def process_song_file(cur, conn, filepath):
    """Reads a song file and fills the users and artists tables
       Input Arguments: cur - cursor, filepath -  filepath of a song file       
    """
    global song_rows
    n_song = 0
    
    # open song file
    try:
        df = pd.read_json(filepath, lines=False)
    except ValueError as e:
        df = pd.read_json(filepath, lines=True)

    print('File: {} ; Records: {}'.format(filepath, len(df)))
    
    for index, row in df.iterrows():
        try:
            # insert song record
            song_data = (row.song_id, row.title, row.artist_id, row.year, row.duration)
            cur.execute(song_table_insert, song_data)
            # insert artist record
            artist_data = (row.artist_id, row.artist_name, row.artist_location,
                           row.artist_latitude, row.artist_longitude)
            cur.execute(artist_table_insert, artist_data)
        except psycopg2.Error as e:
            print(f"Error: No se pudo procesar song_data #record:{index} - {e}")
            conn.rollback()
            n_song = n_song + 1
                    
    # Count the number of row proccesed without errors
    if n_song == 0:
        song_rows += len(df) 
    else:
        song_rows += len(df) - n_song


def process_log_file(cur, conn, filepath):    
    """Reads a log file and fills the time, users and songplay tables
    
       Input Arguments: cur - cursor, filepath -  filepath of a log file       
    """
    error = False
    global log_rows
    
    # open log file    
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']
    
    print('File: {} ; Records: {}'.format(filepath, len(df)))

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')
    
    # insert time data records
    
    # Obtener la semana del año utilizando dt.isocalendar().week
    time_data = list((t.dt.time.tolist(), t.dt.hour.tolist(), t.dt.day.tolist(), t.dt.isocalendar().week.tolist(), 
                      t.dt.month.tolist(), t.dt.year.tolist(), t.dt.weekday.tolist()))


    column_labels = ('timestamp','hour', 'day', 'week', 'month', 'year', 'weekday')
    
    time_df = pd.DataFrame.from_dict({column_labels[0]:time_data[0], column_labels[1]:time_data[1], 
                                      column_labels[2]:time_data[2], column_labels[3]:time_data[3], 
                                      column_labels[4]:time_data[4], column_labels[5]:time_data[5],
                                      column_labels[6]:time_data[6],
                                     })

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e:
            print("Error: Could not insert the row in the time table")
            print(e)
            error = True

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']] 

    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except psycopg2.Error as e:
            print("Error: Could not insert the row in the users table")
            print(e)
            error = True

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms').time(), row.userId, row.level, 
                         songid, artistid, row.sessionId, row.location, row.userAgent)
        
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e:
            print("Error: Could not insert the row in the songs table")
            print(e)
            error = True
            
    # Count the number of row proccesed without errors
    if error == False:
        log_rows += len(df)


def process_data(cur, conn, filepath, func):
    """Reads the directories where the logs and songs files are and gets its names,
       executes the process_song_file or process_log_file funtion
       
       Input Arguments: cur - cursor, conn - database connection, 
                        filepath -  filepath of a directory, 
                        func - process_song_file or process_log_file funtion   
    """
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
        func(cur, conn, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=localhost dbname=sparkifydb user=postgres password=admin")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    
    print('****************************************')
    print('Number of rows proccesed in song_data: {}'.format(song_rows))
    print('****************************************')
    
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    print('****************************************')
    print('Number of rows proccesed in log_data: {}'.format(log_rows))
    print('****************************************')

    conn.close()


if __name__ == "__main__":
    main()