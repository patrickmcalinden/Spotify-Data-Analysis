import json
import pandas as pd
import re 
import requests



#Code could have been under 1/2 methods and condesed a great deal -> I beleive this shows my thought process a bit better

#empty list for data from json
data = []

#loop through json files
for i in range(17):
    # read data
    with open(r"endsong_{}.json".format(i), "r",encoding="utf-8") as f:
        file_data = json.load(f)
    data.extend(file_data)

#create df and append list of data
df = pd.DataFrame.from_records(data)

#remove the "dumb" columns / columns with no track name ("Means that song has been removed or I listened to a podcast 
#not enough data to actually analyze)") format date (ts column)/ and get rid of extra platform info (mostly information that would complicate the analysis and add no benefits)
df = df.drop(columns=['conn_country', 'username', 'user_agent_decrypted', 'incognito_mode', 'offline_timestamp', 'episode_name' , 'episode_show_name', 'spotify_episode_uri','ip_addr_decrypted'])
df = df.dropna(subset=['master_metadata_track_name'])


df = df.astype({'master_metadata_track_name': 'str'})
df['ts'] = pd.to_datetime(df['ts'], format='%Y-%m-%dT%H:%M:%SZ')

df['platform'] = df['platform'].str.split('(').str[0]
df['platform'] = df['platform'].apply(lambda x: re.findall(r";[^ ;]*", x)[0][1:] if re.findall(r";[^ ;]*", x) else x)
df['platform'] = df['platform'].str.split('.').str[0].str.replace("_", " ")
df['spotify_track_uri'] = df['spotify_track_uri'].str.split(':').str[2]


#write the dataframe to a csv file / used for the first test
df.to_csv(r"endsong_data.csv", index=False)



#dictonaries for storing data / used for 
track_artist_mapping = {}
track_song_release_date = {}


#creating batch request (size of 50/request) and inputing track uri and artist id into a dictionary 
#+ adding release date for songs
def batch_requests(url, headers, ids, batch_size=50):
    for i in range(0, len(ids), batch_size):
        ids_batch = ids[i:i+batch_size]
        ids_string = '%2C'.join(ids_batch)
        url_batch = f'{url}?ids={ids_string}'
        response = requests.get(url_batch, headers=headers)
        #check the status code
        if response.status_code != 200:
                print("Error: could not get track information")
                return
        #load data
        data = json.loads(response.text)
        #iterate over json
        for track in data["tracks"]:
            # Get track URI, artist ID, and release date
            track_uri = track["id"]
            artist_id = track["album"]["artists"][0]["id"]
            release_date = track["album"]["release_date"]
            if release_date == "" or release_date == "0000":
                release_date = "NONE" 
                track_song_release_date[track_uri] = release_date
            else:
                release_date = release_date.split("-")[0]
                track_song_release_date[track_uri] = release_date
             # Add track URI and artist ID to dictionary
            track_artist_mapping[track_uri] = artist_id

#api call formatting 
url = 'https://api.spotify.com/v1/tracks'
headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Bearer BQAmD6fBkSWBfJCBtr0-28_6m7yM7QFe3_xC3SFrPj6U7uEU6efvVd-cW4rlXkLWXF5eIrlXfmtiJANDlm_yaJ6WdSeW6VxxmivMNlv4HgOmeDTm916C9HSPz-HsaUEw0XlCBJUCdUc90IY8YGHgaYqr9N1Is8HxNa-iBFCjSteVm0uO"}

#get unique tracks so we don't overload spotify api
unique_tracks = df['spotify_track_uri'].unique()
response_list = batch_requests(url, headers, unique_tracks)

#check
print(track_artist_mapping)


#making copy of dictionary and mapping values to keys and making an new excel column
track_id_to_artist_id = track_artist_mapping.copy()

df['artist_id'] = df['spotify_track_uri'].map(lambda x: track_id_to_artist_id.get(f'{x}', None))
df['release_date'] = df['spotify_track_uri'].map(lambda x: track_song_release_date.get(f'{x}', None))

#changing the position of the artist_id and release_date column
cols = df.columns.tolist()
cols.remove("artist_id")
cols.remove("release_date")
cols.insert(cols.index("master_metadata_album_artist_name") + 1, "artist_id")
cols.insert(cols.index("artist_id") + 1, "release_date")
df = df[cols]

#another requests that gets the genre of the artists
artist_genre_tracking = {}

def batch_requests(url, headers, ids, batch_size=50):
    for i in range(0, len(ids), batch_size):
        ids_batch = ids[i:i+batch_size]
        ids_string = '%2C'.join(ids_batch)
        url_batch = f'{url}?ids={ids_string}'
        response = requests.get(url_batch, headers=headers)
        #check status
        if response.status_code != 200:
                print("Error: could not get track information")
                return
        #Get JSON data
        data = json.loads(response.text)
        #iterate over json
        for artist in data["artists"]:
            #get artist id and genre
            artist_id = artist["id"]
            genre = artist['genres']
            #spent 30 minutes to realize that i was using if != "" for a list...which is impossible...
            if len(genre) == 0:
                genre = "GNA" 
                artist_genre_tracking[artist_id] = genre
            else:
                genre = genre[0]
                artist_genre_tracking[artist_id] = genre

#api call formatting 
url = 'https://api.spotify.com/v1/artists'
headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f"Bearer BQAmD6fBkSWBfJCBtr0-28_6m7yM7QFe3_xC3SFrPj6U7uEU6efvVd-cW4rlXkLWXF5eIrlXfmtiJANDlm_yaJ6WdSeW6VxxmivMNlv4HgOmeDTm916C9HSPz-HsaUEw0XlCBJUCdUc90IY8YGHgaYqr9N1Is8HxNa-iBFCjSteVm0uO"}

#getting unique artist ids
unique_artists = df['artist_id'].unique()
response_list = batch_requests(url, headers, unique_artists)

#fills genre info into all occurances on the dataframe
df['artist_genre'] = df['artist_id'].map(lambda x: artist_genre_tracking.get(f'{x}', None))

#repostioning column
cols = df.columns.tolist()
cols.remove("artist_genre")
cols.insert(cols.index("master_metadata_album_album_name") + 1, "artist_genre")
df = df[cols]

#had a bit of trouble with this...decided to make a method to find the most frequently occuring genres for each artist
#some artists had pop,rock,and missing values attached to their name, decided to overwrite genres that didn't show up to oftern
def update_genres(df):
    # Group data by artist name
    artist_groups = df.groupby("master_metadata_album_artist_name")

    # Iterate over
    for name, group in artist_groups:
        # Get the most common genre
        most_common_genres = group["artist_genre"].mode()
        # check if there are more than one genres
        if len(most_common_genres) > 1:
            most_common_genre = most_common_genres[1] if most_common_genres[0] == "GNA" else most_common_genres[0]
        else:
            most_common_genre = most_common_genres[0]
        # Replace all genres for the artist, with the most common genre
        df.loc[group.index, "artist_genre"] = most_common_genre
    return df
df = update_genres(df)

#create csv for data analysis
df.to_csv(r"final_data.csv", index=False)