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
# Pull player_dobs from database
con = psycopg2.connect(DATABASE_URL)
cur = con.cursor()
age_query = "SELECT * FROM player_dobs;"
player_dobs = pd.read_sql(age_query, con)
# Check for any players whose ages are not already in the db
to_query_age = player_df[~player_df['PlayerID'].isin(player_dobs['PlayerID'])]
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
player_df = player_df.drop(columns='DOB')
player_df = player_df.merge(shark_df, on='PlayerID', how='left').merge(adp_df, on='PlayerID', how='left')
player_df['SharkRank'].fillna(3000, inplace=True)
player_df['ADP'].fillna(3000, inplace=True)
player_df = player_df.sort_values(by=['SharkRank'])
player_df.reset_index(inplace=True, drop=True)  

# Split player df by player position
qbs = player_df[player_df['Position'] == "QB"]
qbs.reset_index(inplace=True, drop=True)
rbs = player_df[player_df['Position'] == "RB"]
rbs.reset_index(inplace=True, drop=True)
wrs = player_df[player_df['Position'] == "WR"]
wrs.reset_index(inplace=True, drop=True)
tes = player_df[player_df['Position'] == "TE"]
tes.reset_index(inplace=True, drop=True)
pks = player_df[player_df['Position'] == "PK"]
pks.reset_index(inplace=True, drop=True)
defs = player_df[player_df['Position'] == "Def"]
defs.reset_index(inplace=True, drop=True)

# Get Point Projections
def get_point_projections():
    connection = False
    try:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        cursor = conn.cursor()
        query = 'SELECT * FROM point_projections'
        cursor.execute(query)
        point_projections = pd.read_sql(query, conn)
        return point_projections
    except (Exception, Error) as error:
        print(error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Connection to python_app database has now been closed")
point_projections = get_point_projections()

# Split point_projection df by player position
qb_proj = point_projections[point_projections['Position'] == "QB"]
qb_proj.reset_index(inplace=True, drop=True)
rb_proj = point_projections[point_projections['Position'] == "RB"]
rb_proj.reset_index(inplace=True, drop=True)
wr_proj = point_projections[point_projections['Position'] == "WR"]
wr_proj.reset_index(inplace=True, drop=True)
te_proj = point_projections[point_projections['Position'] == "TE"]
te_proj.reset_index(inplace=True, drop=True)
pk_proj = point_projections[point_projections['Position'] == "PK"]
pk_proj.reset_index(inplace=True, drop=True)
def_proj = point_projections[point_projections['Position'] == "Def"]
def_proj.reset_index(inplace=True, drop=True)

# Join dfs for current year to point_projection dfs
# Merge dfs
qbs = pd.merge(qbs, qb_proj, how="left", left_index=True, right_index=True)
rbs = pd.merge(rbs, rb_proj, how="left", left_index=True, right_index=True)
wrs = pd.merge(wrs, wr_proj, how="left", left_index=True, right_index=True)
tes = pd.merge(tes, te_proj, how="left", left_index=True, right_index=True)
pks = pd.merge(pks, pk_proj, how="left", left_index=True, right_index=True)
defs = pd.merge(defs, def_proj, how="left", left_index=True, right_index=True)

player_df = pd.concat([qbs, rbs, wrs, tes, pks, defs])
player_df = player_df.sort_values(by=['Projection_Relative'])

# Write player_df to database
engine = create_engine(DATABASE_URL, echo = False)
player_df.to_sql("player_df", con=engine, if_exists='replace', index=False)
# Write ages to db
player_dobs.to_sql("player_dobs", con=engine, if_exists='replace', index=False)