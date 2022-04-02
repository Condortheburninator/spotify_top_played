from concurrent.futures import Executor
from numpy import append
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3
import config

# constants should be capitals
DATABASE_LOCATION   = 'sqlite:///my_played_tracks.sqlite'
USER_ID             = config.USER_ID
TOKEN               = config.TOKEN

# pandas options
pd.set_option('display.max_rows', None)

def check_if_valid_data(df: pd.DataFrame) -> bool:

    if df.empty:
        print('no songs downloaded')
        return False

    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception('primary key constraint issue')

    # check for nulls
    if df.isnull().values.any():
        raise Exception('null values detected')

    yesterday = datetime.datetime.now() - datetime.timedelta(days = 1)
    yesterday = yesterday.replace(hour = 0, minute = 0, microsecond = 0)

    timestamps = df['timestamp'].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception('at least one song wasn''t from yesterday')
    
    return True


if __name__ == "__main__":

    # Extract part of the ETL process
    headers = {

        "Accept"        : "application/json",
        "Content-Type"  : "application/json",
        "Authorization" : "Bearer {token}".format(token = TOKEN)
        # "Authorization" : "Bearer {token}"
    }
    
    # Convert time to Unix timestamp in miliseconds
    today                       = datetime.datetime.now()
    yesterday                   = today - datetime.timedelta(days = 1)
    yesterday_unix_timestamp    = int(yesterday.timestamp()) * 1000

    # Download all songs you've listened to "after yesterday", which means in the last 24 hours
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time = yesterday_unix_timestamp), headers = headers)

    data = r.json()

    # print(data)

    song_names      = []
    artist_names    = []
    played_at_list  = []
    timestamps      = []

    # Extracting only the relevant bits of data from the json object
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    # Prepare a dictionary in order to turn it into a pandas dataframe below
    song_dict = {

         "song_name"     : song_names
        ,"artist_name"   : artist_names
        ,"played_at"     : played_at_list
        ,"timestamp"     : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])

    # validate data

    # if check_if_valid_data(song_df):
    #     print('data valid, LOAD data')

    print(song_df)

    # LOAD data

    engine  = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn    = sqlite3.connect('my_played_tracks.sqlite')
    cursor  = conn.cursor()

    sql_query = '''
                        CREATE TABLE IF NOT EXISTS PlayedTracks (

                             NameSong       VARCHAR(300)
                            ,NameArtist     VARCHAR(300)
                            ,PlayedAt       VARCHAR(300)
                            ,TimeStamp      DATETIME
                            ,CONSTRAINT primary_key_constraint PRIMARY KEY (PlayedAt)
                        )
                '''

    cursor.execute(sql_query)
    print('table created successfully')

    try:
        song_df.to_sql('my_played_tracks', engine, index = False, if_exists = 'append')
    except:
        print('data already exists in the database')

    conn.close()