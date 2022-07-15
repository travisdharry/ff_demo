# Import dependencies
from flask import Flask, render_template, request
import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import date
import json
import os
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime, date
from dateutil.relativedelta import *

# Find environment variables
DATABASE_URL = os.environ.get("DATABASE_URL", None)
# sqlalchemy deprecated urls which begin with "postgres://"; now it needs to start with "postgresql://"
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)


# Get all players' name, team name, position
urlString = "https://api.myfantasyleague.com/2022/export?TYPE=players"
response = requests.get(urlString)
soup = BeautifulSoup(response.content,'xml')
data = []
players = soup.find_all('player')
for i in range(len(players)):
    rows = [players[i].get("id"), players[i].get("name"), players[i].get("position"), players[i].get("team")]
    data.append(rows)
player_df = pd.DataFrame(data)
player_df.columns=['PlayerID','Name', 'Position', 'Team']

# Get Shark Ranks
urlString = "https://api.myfantasyleague.com/2022/export?TYPE=playerRanks"
response = requests.get(urlString)
soup = BeautifulSoup(response.content,'xml')
data = []
sharkRanks = soup.find_all('player')
for i in range(len(sharkRanks)):
    rows = [sharkRanks[i].get("id"), sharkRanks[i].get("rank")]
    data.append(rows)
shark_df = pd.DataFrame(data)
shark_df.columns=['PlayerID','SharkRank']
shark_df['SharkRank'] = shark_df['SharkRank'].astype('int32')

# Get adp
urlString = "https://api.myfantasyleague.com/2022/export?TYPE=adp"
response = requests.get(urlString)
soup = BeautifulSoup(response.content,'xml')
data = []
players = soup.find_all('player')
for i in range(len(players)):
    rows = [players[i].get("id"), players[i].get("averagePick")]
    data.append(rows)
adp_df = pd.DataFrame(data)
adp_df.columns=['PlayerID','ADP']
adp_df['ADP'] = adp_df['ADP'].astype('float32')

# Get any player ages that are not already in the db
# Pull age_df from database
con = psycopg2.connect(DATABASE_URL)
cur = con.cursor()
age_query = "SELECT * FROM player_dobs;"
player_dobs = pd.read_sql(age_query, con)
# Check for any players whose ages are not already in the db
to_query_age = player_df[~player_df['PlayerID'].isin(age_df['PlayerID'])]
if len(to_query_age)>0:
    # Break player list into chunks small enough for the API server
    n = 50  #chunk row size
    list_df = [to_query_age.PlayerID[i:i+n] for i in range(0,to_query_age.PlayerID.shape[0],n)]

    for i in range(len(list_df)):
        idList = ",".join(list_df[i])

        # Get playerProfiles
        urlString = f"https://api.myfantasyleague.com/2022/export?TYPE=playerProfile&P={idList}"
        response = requests.get(urlString)
        soup = BeautifulSoup(response.content,'xml')
        data = []
        profiles = soup.find_all('playerProfile')
        players = soup.find_all('player')
        for i in range(len(profiles)):
            rows = [profiles[i].get("id"), players[i].get("dob")]
            data.append(rows)
        data_df = pd.DataFrame(data)
        age = pd.DataFrame(columns=['PlayerID', 'DOB'])
        age['PlayerID'] = data_df[0]
        age['DOB'] = data_df[1]
        player_dobs = player_dobs.append(age)

# Convert string to datetime
player_dobs['DOB'] = pd.to_datetime(player_dobs['DOB'])
# Convert DOB to Age
today = date.today()
def age(born):
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))
player_dobs['Age'] = player_dobs['DOB'].apply(age)

# Merge all dfs
player_df = player_df.merge(player_dobs, on='PlayerID', how='left')  
player_df = player_df.merge(shark_df, on='PlayerID', how='left').merge(adp_df, on='PlayerID', how='left')
player_df['SharkRank'].fillna(3000, inplace=True)
player_df['ADP'].fillna(3000, inplace=True)
player_df = player_df.sort_values(by=['SharkRank'])
player_df.reset_index(inplace=True, drop=True)  

# Write player_df to database
engine = create_engine(DATABASE_URL, echo = False)
player_df.to_sql("player_df", con=engine, if_exists='replace', index=False)
# Write ages to db
player_dobs.to_sql("player_dobs", con=engine, if_exists='replace', index=False)