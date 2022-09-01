# %%
### Import dependencies
# Dependencies for data manipulation
import pandas as pd
import numpy as np
import os
from datetime import datetime, date
from dateutil.relativedelta import *

# Dependencies for Databases
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors
from sqlalchemy import create_engine

# Dependencies for APIs
from bs4 import BeautifulSoup
import requests
import json

# Dependencies for Webscraping
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

# Dependencies for random forest model
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from joblib import dump, load

# Internal imports
from db import get_df

# Find environment variables
DATABASE_URL = os.environ.get("DATABASE_URL", None)
# sqlalchemy deprecated urls which begin with "postgres://"; now it needs to start with "postgresql://"
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)


# %%
# Get all players' name, team name, position
urlString = "https://api.myfantasyleague.com/2022/export?TYPE=players"
response = requests.get(urlString)
soup = BeautifulSoup(response.content,'xml')
data = []
players = soup.find_all('player')
for i in range(len(players)):
    rows = [players[i].get("id"), players[i].get("name"), players[i].get("position"), players[i].get("team")]
    data.append(rows)
scrape1 = pd.DataFrame(data)
scrape1.columns=['PlayerID','Name', 'Position', 'Team']

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

# Get ADP
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

# Get player ages
# Get any player dobs who are already in the db
player_dobs = get_df('player_dobs')
# Check for any players whose ages are not already in the db
to_query_age = scrape1[~scrape1['PlayerID'].isin(player_dobs['PlayerID'])]
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

# Merge all dfs from MyFantasyLeague API
scrape1 = scrape1.merge(player_dobs, on='PlayerID', how='left')
scrape1 = scrape1.drop(columns='DOB')
scrape1 = scrape1.merge(shark_df, on='PlayerID', how='left').merge(adp_df, on='PlayerID', how='left')
scrape1['SharkRank'].fillna(3000, inplace=True)
scrape1['ADP'].fillna(3000, inplace=True)
scrape1 = scrape1.sort_values(by=['SharkRank'])
scrape1.reset_index(inplace=True, drop=True)  

### Clean MFL data
## Select only relevant positions
scrape1 = scrape1.loc[scrape1['Position'].isin(['QB', 'WR', 'RB', 'TE', 'PK', 'Def'])]
scrape1 = scrape1.reset_index(drop=True)
## Clean Name column
to_join = scrape1['Name'].str.split(", ", n=1, expand=True)
to_join.columns = ['lname', 'fname']
to_join['Name'] = to_join['fname'] + " " + to_join['lname']
scrape1['Name'] = to_join['Name']
# Change name to Title Case
scrape1['Name'] = scrape1['Name'].str.upper()
# Drop name punctuation
scrape1['Name'] = scrape1['Name'].str.replace(".", "")
scrape1['Name'] = scrape1['Name'].str.replace(",", "")
scrape1['Name'] = scrape1['Name'].str.replace("'", "")
## Clean position column
scrape1['Position'] = scrape1['Position'].replace('Def', 'DF')
# Clean Team column
scrape1['Team'] = scrape1['Team'].replace('FA*', 'FA')
## Change column names
scrape1.columns = ['id_mfl', 'player', 'pos_mfl', 'team', 'age', 'sharkRank', 'adp']
scrape1

# %%
### scrape posRanks
# Set Selenium/Chrome settings
chrome_options = webdriver.ChromeOptions()
chrome_options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--no-sandbox")
capa = DesiredCapabilities.CHROME
capa["pageLoadStrategy"] = "none"
driver = webdriver.Chrome(
    executable_path=os.environ.get("CHROMEDRIVER_PATH"), 
    chrome_options=chrome_options, 
    desired_capabilities=capa)

# scrape web for stats
url = f"https://www.ourlads.com/nfldepthcharts/depthcharts.aspx"
wait = WebDriverWait(driver, 20)
driver.get(url)
wait.until(EC.presence_of_element_located((By.XPATH, "//table[@id='ctl00_phContent_gvChart']")))
driver.execute_script("window.stop();")

scrape2 = pd.read_html(driver.find_element(By.XPATH, value="//table[@id='ctl00_phContent_gvChart']").get_attribute("outerHTML"))
scrape2 = scrape2[0]

# %%
### Clean scrape2d data
scrape2 = scrape2[['Team', 'Pos', 'Player 1', 'Player 2','Player 3', 'Player 4', 'Player 5']]

# Transform columns into rows
scrape21 = scrape2[['Team', 'Pos', 'Player 1']]
scrape21 = scrape21.rename(columns={'Player 1':'Player'})
scrape21['posRank'] = "1"

scrape22 = scrape2[['Team', 'Pos', 'Player 2']]
scrape22 = scrape22.rename(columns={'Player 2':'Player'})
scrape22['posRank'] = "2"

scrape23 = scrape2[['Team', 'Pos', 'Player 3']]
scrape23 = scrape23.rename(columns={'Player 3':'Player'})
scrape23['posRank'] = "3"

scrape24 = scrape2[['Team', 'Pos', 'Player 4']]
scrape24 = scrape24.rename(columns={'Player 4':'Player'})
scrape24['posRank'] = "4"

scrape25 = scrape2[['Team', 'Pos', 'Player 5']]
scrape25 = scrape25.rename(columns={'Player 5':'Player'})
scrape25['posRank'] = "5"

scrape2_complete = pd.concat([scrape21, scrape22, scrape23, scrape24, scrape25], axis=0, ignore_index=True)

# Clean Position column
# Select only relevant positions
posList = ['LWR', 'RWR', 'SWR', 'TE', 'QB', 'RB', 'PK', 'PR', 'KR', 'RES']
scrape2_final = scrape2_complete.loc[scrape2_complete['Pos'].isin(posList)]
# Convert WR roles to "WR"
scrape2_final['Pos'].replace(["LWR", "RWR", "SWR"], "WR", inplace=True)
scrape2_final['posRank'] = scrape2_final['Pos'] + scrape2_final['posRank']
scrape2_final = scrape2_final.reset_index(drop=True)
scrape2_final.dropna(inplace=True)
scrape2_final.drop_duplicates(subset=['Player', 'Team', 'Pos'], inplace=True)

# Create columns for KRs and PRs
krs = scrape2_final.loc[scrape2_final.Pos=='KR']
krs = krs.drop(columns=['Pos'])
krs.columns = ['Team', 'Player', 'KR']
prs = scrape2_final.loc[scrape2_final.Pos=='PR']
prs = prs.drop(columns=['Pos'])
prs.columns = ['Team', 'Player', 'PR']
# Join pr and pk scrape2s back onto main ourlads scrape2
scrape2_final = scrape2_final.merge(krs, how='left', on=['Player', 'Team']).merge(prs, how='left', on=['Player', 'Team'])
scrape2_final['KR'].fillna("NO", inplace=True)
scrape2_final['PR'].fillna("NO", inplace=True)

# Clean name column
names = scrape2_final['Player'].str.split(" ", n=2, expand=True)
names.columns = ['a', 'b', 'c']
names['a'] = names['a'].str.replace(",", "")
scrape2_final['Player'] = names['b'] + " " + names['a']
# Change to Upper Case
scrape2_final['Player'] = scrape2_final['Player'].str.upper()
# Drop punctuation
scrape2_final['Player'] = scrape2_final['Player'].str.replace(".", "")
scrape2_final['Player'] = scrape2_final['Player'].str.replace(",", "")
scrape2_final['Player'] = scrape2_final['Player'].str.replace("'", "")

# Change column names and order
scrape2_final = scrape2_final[['Player', 'Pos', 'Team', 'posRank', 'KR', 'PR']]
scrape2_final.columns = ['player', 'pos_ol', 'team', 'posRank', 'KR', 'PR']

# Remove separate rows for PRs and KRs
scrape2_final = scrape2_final.loc[(scrape2_final.pos_ol!="KR")]
scrape2_final = scrape2_final.loc[(scrape2_final.pos_ol!="PR")]

# Drop position column
scrape2_final.drop(columns=['pos_ol'], inplace=True)
scrape2_final

# Rename team abbreviations
teamDict = {
    'ARZ':'ARI', 'ATL':'ATL', 'BAL':'BAL', 'BUF':'BUF', 'CAR':'CAR', 'CHI':'CHI', 'CIN':'CIN', 'CLE':'CLE', 
    'DAL':'DAL', 'DEN':'DEN', 'DET':'DET', 'GB':'GBP', 'HOU':'HOU', 'IND':'IND', 'JAX':'JAC', 'KC':'KCC', 
    'LAC':'LAC', 'LAR':'LAR', 'LV':'LVR', 'MIA':'MIA', 'MIN':'MIN', 'NE':'NEP', 'NO':'NOS', 'NYG':'NYG', 
    'NYJ':'NYJ', 'PHI':'PHI', 'PIT':'PIT', 'SEA':'SEA', 'SF':'SFO', 'TB':'TBB', 'TEN':'TEN', 'WAS':'WAS'
    }
scrape2_final['team'] = scrape2_final['team'].map(teamDict)

# %%
### Merge MyFantasyLeague data with scrape2d data
player_df = scrape1.merge(scrape2_final, how='left', on=['player', 'team'])
## Clean merged df
player_df.loc[player_df['pos_mfl']=='DF', 'posRank'] = "DF1"
player_df['KR'].fillna("NO", inplace=True)
player_df['PR'].fillna("NO", inplace=True)
## Clean posRanks
player_df['posRank'] = player_df['posRank'].map({
    'RES1':'RES',
    'RES2':'RES',
    'RES3':'RES',
    'RES4':'RES',
    'RES5':'RES',
    'QB1':'QB1', 
    'QB2':'QB2', 
    'QB3':'QB3', 
    'QB4':'QB3',
    'QB5':'QB3', 
    'RB1':'RB1', 
    'RB2':'RB2', 
    'RB3':'RB3', 
    'RB4':'RB3', 
    'RB5':'RB3',
    'WR1': 'WR1', 
    'WR2': 'WR2', 
    'WR3': 'WR3', 
    'WR4': 'WR3', 
    'WR5': 'WR3', 
    'TE1':'TE1', 
    'TE2':'TE2', 
    'TE3':'TE3', 
    'TE4':'TE3', 
    'TE5':'TE3', 
    'PK1':'PK1', 
    'PK2':'PK2', 
    'PK3':'PK3',
    'DF1':'DF1'
    })
# Create "RES/NO" column
player_df['RES'] = "NO"
player_df.loc[player_df['posRank']=="RES", 'RES'] = "RES"
player_df.loc[player_df.posRank.isna(), 'posRank'] = player_df.loc[player_df.posRank.isna(), 'pos_mfl'] + "3"
player_df.loc[player_df.posRank=="RES", 'posRank'] = player_df.loc[player_df.posRank=="RES", 'pos_mfl'] + "3"
# Specify all players are in current season
player_df['season'] = 2022

# %%
### Get historical data
prior1 = get_df('prior1')
prior2 = get_df('prior2')


# %%
# Create current_df
# This will mean scraping the ff db site weekly
curr = pd.DataFrame(player_df['player'])
colList = ['gamesPlayed',
    'passA', 'passC', 'passY', 'passT', 'passI', 'pass2', 
    'rushA', 'rushY','rushT', 'rush2', 
    'recC', 'recY', 'recT', 'rec2', 'fum', 
    'XPA', 'XPM','FGA', 'FGM', 'FG50', 
    'defSack', 'defI', 'defSaf', 'defFum', 'defBlk','defT', 'defPtsAgainst', 'defPassYAgainst', 'defRushYAgainst','defYdsAgainst'
]
cols = pd.DataFrame(columns=colList)
curr = curr.merge(cols, how='left', left_index=True, right_index=True)
curr.fillna(0, inplace=True)

# Rename all columns in curr
colList = [(x + "_curr") for x in list(curr.columns)]
curr.columns = colList
curr = curr.rename(columns={
       'player_curr':'player',
       })
curr

# %%
# Merge playerdf, currentdf, prior1, and prior2
player_df = player_df.merge(curr, how='left', on='player').merge(prior1, how='left', on='player').merge(prior2, how='left', on='player')
# Fill data for players who do not have prior data
player_df.fillna(0, inplace=True)

# %%
player_df.drop_duplicates(subset=['player', 'pos_mfl'], inplace=True)

# %%
# Get schedule
schedule = get_df('schedule')
# Merge in opponents
player_df = player_df.merge(schedule, how='left', on='team')
player_df

# %%
# Rename position column in player_df
player_df.rename(columns={'pos_mfl':'pos'}, inplace=True)

# %%
# Get opponent historical data
# select only defenses
allDef = player_df.loc[player_df['pos']=='DF']
allDef

# %%

# Get current defensive scores
currDef = allDef.copy()
# Select only relevant columns
currDef = currDef[['team', 'week',
       'defSack_curr', 'defI_curr',
       'defSaf_curr', 'defFum_curr', 'defBlk_curr',
       'defT_curr', 'defPtsAgainst_curr', 'defPassYAgainst_curr',
       'defRushYAgainst_curr', 'defYdsAgainst_curr']]

# Get prior defensive scores
priorDef = allDef.copy()
# Select only relevant columns
priorDef = priorDef[['team', 'week',
       'defSack_prior1', 'defI_prior1',
       'defSaf_prior1', 'defFum_prior1', 'defBlk_prior1',
       'defT_prior1', 'defPtsAgainst_prior1', 'defPassYAgainst_prior1',
       'defRushYAgainst_prior1', 'defYdsAgainst_prior1']]
# Merge the two defensive dfs
allDef = currDef.merge(priorDef, how='left', on=['team', 'week'])

# Rename all columns in allDef
colList = [(x + "_opp") for x in list(allDef.columns)]
allDef.columns = colList
allDef = allDef.rename(columns={
       'team_opp':'opponent',
       'week_opp':'week'
       })

# %%
# Connect opponents to defenses
player_df = player_df.merge(allDef, how='left', on=['opponent', 'week'])
print(len(player_df))

# %%

player_df = player_df[[
    'id_mfl',
    'season',
    'week',
    'team',
    'player',
    'age',
    'sharkRank', 
    'adp',
    'KR',
    'PR',
    'RES',
    'pos',
    'posRank',
    'opponent',
    'passA_curr',
    'passC_curr',
    'passY_curr',
    'passT_curr',
    'passI_curr',
    'pass2_curr',
    'rushA_curr',
    'rushY_curr',
    'rushT_curr',
    'rush2_curr',
    'recC_curr',
    'recY_curr',
    'recT_curr',
    'rec2_curr',
    'fum_curr',
    'XPA_curr',
    'XPM_curr',
    'FGA_curr',
    'FGM_curr',
    'FG50_curr',
    'defSack_curr',
    'defI_curr',
    'defSaf_curr',
    'defFum_curr',
    'defBlk_curr',
    'defT_curr',
    'defPtsAgainst_curr',
    'defPassYAgainst_curr',
    'defRushYAgainst_curr',
    'defYdsAgainst_curr',
    'gamesPlayed_curr',
    'gamesPlayed_prior1',
    'passA_prior1',
    'passC_prior1',
    'passY_prior1',
    'passT_prior1',
    'passI_prior1',
    'pass2_prior1',
    'rushA_prior1',
    'rushY_prior1',
    'rushT_prior1',
    'rush2_prior1',
    'recC_prior1',
    'recY_prior1',
    'recT_prior1',
    'rec2_prior1',
    'fum_prior1',
    'XPA_prior1',
    'XPM_prior1',
    'FGA_prior1',
    'FGM_prior1',
    'FG50_prior1',
    'defSack_prior1',
    'defI_prior1',
    'defSaf_prior1',
    'defFum_prior1',
    'defBlk_prior1',
    'defT_prior1',
    'defPtsAgainst_prior1',
    'defPassYAgainst_prior1',
    'defRushYAgainst_prior1',
    'defYdsAgainst_prior1',
    'gamesPlayed_prior2',
    'passA_prior2',
    'passC_prior2',
    'passY_prior2',
    'passT_prior2',
    'passI_prior2',
    'pass2_prior2',
    'rushA_prior2',
    'rushY_prior2',
    'rushT_prior2',
    'rush2_prior2',
    'recC_prior2',
    'recY_prior2',
    'recT_prior2',
    'rec2_prior2',
    'fum_prior2',
    'XPA_prior2',
    'XPM_prior2',
    'FGA_prior2',
    'FGM_prior2',
    'FG50_prior2',
    'defSack_prior2',
    'defI_prior2',
    'defSaf_prior2',
    'defFum_prior2',
    'defBlk_prior2',
    'defT_prior2',
    'defPtsAgainst_prior2',
    'defPassYAgainst_prior2',
    'defRushYAgainst_prior2',
    'defYdsAgainst_prior2',
    'defSack_curr_opp',
    'defI_curr_opp',
    'defSaf_curr_opp',
    'defFum_curr_opp',
    'defBlk_curr_opp',
    'defT_curr_opp',
    'defPtsAgainst_curr_opp',
    'defPassYAgainst_curr_opp',
    'defRushYAgainst_curr_opp',
    'defYdsAgainst_curr_opp',
    'defSack_prior1_opp',
    'defI_prior1_opp',
    'defSaf_prior1_opp',
    'defFum_prior1_opp',
    'defBlk_prior1_opp',
    'defT_prior1_opp',
    'defPtsAgainst_prior1_opp',
    'defPassYAgainst_prior1_opp',
    'defRushYAgainst_prior1_opp',
    'defYdsAgainst_prior1_opp']]

# %%
labels = [
    'passA', 'passC', 'passY', 'passT', 'passI', 'pass2', 
    'rushA', 'rushY', 'rushT', 'rush2', 
    'recC', 'recY', 'recT', 'rec2', 'fum', 
    'XPA', 'XPM', 'FGA', 'FGM', 'FG50', 
    'defSack', 'defI', 'defSaf', 'defFum', 'defBlk', 'defT', 
    'defPtsAgainst', 'defPassYAgainst', 'defRushYAgainst', 'defYdsAgainst'  
]

features = [
    'week', 'age', 
    'passA_curr', 'passC_curr', 'passY_curr', 'passT_curr', 'passI_curr', 'pass2_curr', 
    'rushA_curr', 'rushY_curr', 'rushT_curr', 'rush2_curr', 
    'recC_curr', 'recY_curr', 'recT_curr', 'rec2_curr', 'fum_curr', 
    'XPA_curr', 'XPM_curr', 'FGA_curr', 'FGM_curr', 'FG50_curr', 
    'defSack_curr', 'defI_curr', 'defSaf_curr', 'defFum_curr', 'defBlk_curr', 'defT_curr', 
    'defPtsAgainst_curr', 'defPassYAgainst_curr', 'defRushYAgainst_curr', 'defYdsAgainst_curr', 
    'gamesPlayed_curr', 
    'gamesPlayed_prior1', 
    'passA_prior1', 'passC_prior1', 'passY_prior1', 'passT_prior1', 'passI_prior1', 'pass2_prior1', 
    'rushA_prior1', 'rushY_prior1', 'rushT_prior1', 'rush2_prior1', 
    'recC_prior1', 'recY_prior1', 'recT_prior1', 'rec2_prior1', 'fum_prior1', 
    'XPA_prior1', 'XPM_prior1', 'FGA_prior1', 'FGM_prior1', 'FG50_prior1', 
    'defSack_prior1', 'defI_prior1', 'defSaf_prior1', 'defFum_prior1', 'defBlk_prior1', 'defT_prior1', 
    'defPtsAgainst_prior1', 'defPassYAgainst_prior1', 'defRushYAgainst_prior1', 'defYdsAgainst_prior1', 
    'gamesPlayed_prior2', 
    'passA_prior2', 'passC_prior2', 'passY_prior2', 'passT_prior2', 'passI_prior2', 'pass2_prior2', 
    'rushA_prior2', 'rushY_prior2', 'rushT_prior2', 'rush2_prior2', 
    'recC_prior2', 'recY_prior2', 'recT_prior2', 'rec2_prior2', 'fum_prior2', 
    'XPA_prior2', 'XPM_prior2', 'FGA_prior2', 'FGM_prior2', 'FG50_prior2', 
    'defSack_prior2', 'defI_prior2', 'defSaf_prior2', 'defFum_prior2', 'defBlk_prior2', 'defT_prior2', 
    'defPtsAgainst_prior2', 'defPassYAgainst_prior2', 'defRushYAgainst_prior2', 'defYdsAgainst_prior2', 
    'defSack_curr_opp', 'defI_curr_opp', 'defSaf_curr_opp', 'defFum_curr_opp', 'defBlk_curr_opp', 'defT_curr_opp', 
    'defPtsAgainst_curr_opp', 'defPassYAgainst_curr_opp', 'defRushYAgainst_curr_opp', 'defYdsAgainst_curr_opp', 
    'defSack_prior1_opp', 'defI_prior1_opp', 'defSaf_prior1_opp', 'defFum_prior1_opp', 'defBlk_prior1_opp', 'defT_prior1_opp', 
    'defPtsAgainst_prior1_opp', 'defPassYAgainst_prior1_opp', 'defRushYAgainst_prior1_opp', 'defYdsAgainst_prior1_opp', 
    'pos', 'posRank'
]


# %%
### WR Predictions
# Read player model and ages
xl2 = player_df.copy()
# Select only one player position
xl2 = xl2.loc[xl2.posRank.isin(['WR1', 'WR2', 'WR3'])]
xl2 = xl2.loc[xl2.pos=='WR']
xl2 = xl2.dropna()
xl2.reset_index(inplace=True, drop=True)

# Select features
X = xl2[features]
header = xl2[[
    'id_mfl',
    'season',
    'week',
    'team',
    'player',
    'age',
    'sharkRank', 
    'adp',
    'KR',
    'PR',
    'RES',
    'pos',
    'posRank',
    'opponent'
]]

# Encode categorical features
X = pd.get_dummies(X, columns = ['pos', 'posRank'])

# Check if there were the correct number of posRanks in the dataset
for rank in ["posRank_WR1", "posRank_WR2", "posRank_WR3"]:
    if rank not in list(X.columns):
        X[rank] = 0

# Make sure we have the necessary columns
X = X[['week', 'age', 'passA_curr', 'passC_curr', 'passY_curr', 'passT_curr', 'passI_curr', 'pass2_curr', 'rushA_curr', 'rushY_curr', 
'rushT_curr', 'rush2_curr', 'recC_curr', 'recY_curr', 'recT_curr', 'rec2_curr', 'fum_curr', 'XPA_curr', 'XPM_curr', 'FGA_curr', 'FGM_curr', 
'FG50_curr', 'defSack_curr', 'defI_curr', 'defSaf_curr', 'defFum_curr', 'defBlk_curr', 'defT_curr', 'defPtsAgainst_curr', 
'defPassYAgainst_curr', 'defRushYAgainst_curr', 'defYdsAgainst_curr', 'gamesPlayed_curr', 'gamesPlayed_prior1', 'passA_prior1', 
'passC_prior1', 'passY_prior1', 'passT_prior1', 'passI_prior1', 'pass2_prior1', 'rushA_prior1', 'rushY_prior1', 'rushT_prior1', 
'rush2_prior1', 'recC_prior1', 'recY_prior1', 'recT_prior1', 'rec2_prior1', 'fum_prior1', 'XPA_prior1', 'XPM_prior1', 'FGA_prior1', 
'FGM_prior1', 'FG50_prior1', 'defSack_prior1', 'defI_prior1', 'defSaf_prior1', 'defFum_prior1', 'defBlk_prior1', 'defT_prior1', 
'defPtsAgainst_prior1', 'defPassYAgainst_prior1', 'defRushYAgainst_prior1', 'defYdsAgainst_prior1', 'gamesPlayed_prior2', 'passA_prior2', 
'passC_prior2', 'passY_prior2', 'passT_prior2', 'passI_prior2', 'pass2_prior2', 'rushA_prior2', 'rushY_prior2', 'rushT_prior2', 
'rush2_prior2', 'recC_prior2', 'recY_prior2', 'recT_prior2', 'rec2_prior2', 'fum_prior2', 'XPA_prior2', 'XPM_prior2', 'FGA_prior2', 
'FGM_prior2', 'FG50_prior2', 'defSack_prior2', 'defI_prior2', 'defSaf_prior2', 'defFum_prior2', 'defBlk_prior2', 'defT_prior2', 
'defPtsAgainst_prior2', 'defPassYAgainst_prior2', 'defRushYAgainst_prior2', 'defYdsAgainst_prior2', 'defSack_curr_opp', 'defI_curr_opp', 
'defSaf_curr_opp', 'defFum_curr_opp', 'defBlk_curr_opp', 'defT_curr_opp', 'defPtsAgainst_curr_opp', 'defPassYAgainst_curr_opp', 
'defRushYAgainst_curr_opp', 'defYdsAgainst_curr_opp', 'defSack_prior1_opp', 'defI_prior1_opp', 'defSaf_prior1_opp', 'defFum_prior1_opp', 
'defBlk_prior1_opp', 'defT_prior1_opp', 'defPtsAgainst_prior1_opp', 'defPassYAgainst_prior1_opp', 'defRushYAgainst_prior1_opp', 
'defYdsAgainst_prior1_opp', 'pos_WR', 'posRank_WR1', 'posRank_WR2', 'posRank_WR3']]

#load saved model
regressor = load('models/rfmodel_WR1.joblib')

# Run model
y_pred = regressor.predict(X)
y_pred = pd.DataFrame(y_pred)
y_pred.columns = labels
y_pred

# Calculate FANTASY scores
# Define scoring multiplier based on league settings
multiplier = [
    0,0,.04,4,-2,2,.1,.1,6,2,.25,.1,6,2,-2,0,1,0,3,5,1,2,2,2,1.5,6,0,0,0,0,1,1
]
# Define bins for defensive PointsAgainst and YardsAgainst based on MFL scoring categories
binList_defPts = [-5,0,6,13,17,21,27,34,45,59,99]
binList_defYds = [0,274,324,375,425,999]
# Define correlating scores for defensive PointsAgainst and YardsAgainst based on league settings
ptList_defPts = [10,8,7,5,3,2,0,-1,-3,-5]
ptList_defYds = [5,2,0,-2,-5]
# Bin and cut the defensive predictions
y_pred['defPtsBin'] = pd.cut(y_pred['defPtsAgainst'], bins=binList_defPts, include_lowest=True, labels=ptList_defPts)
y_pred['defYdsBin'] = pd.cut(y_pred['defYdsAgainst'], bins=binList_defYds, include_lowest=True, labels=ptList_defYds)
# Merge predictions with header columns so we know the players' position
a_pred = header.merge(y_pred, left_index=True, right_index=True)
# Assign value of zero to all non-defensive players' bins
a_pred.loc[a_pred['pos']!='DF', 'defPtsBin'] = 0
a_pred.loc[a_pred['pos']!='DF', 'defYdsBin'] = 0
# Drop the header columns again
a_pred = a_pred.drop(columns=['id_mfl', 'week','season','team','player','age','sharkRank','adp','pos','KR','PR','RES','posRank','opponent'])
# Create function to apply scoring multiplier
def multer(row):
    return row.multiply(multiplier)
# Apply scoring multiplier to predictions
c = a_pred.apply(multer, axis=1)
c = c.apply(np.sum, axis=1)
c = pd.DataFrame(c, columns=['pred'])

# Merge header columns with predictions
WRdf = header.merge(c, left_index=True, right_index=True)

# %%
### RB Predictions
# Read player model and ages
xl2 = player_df.copy()
# Select only one player position
xl2 = xl2.loc[xl2.posRank.isin(['RB1', 'RB2', 'RB3'])]
xl2 = xl2.loc[xl2.pos=='RB']
xl2 = xl2.dropna()
xl2.reset_index(inplace=True, drop=True)

# Select features
X = xl2[features]
header = xl2[[
    'id_mfl',
    'season',
    'week',
    'team',
    'player',
    'age',
    'sharkRank', 
    'adp',
    'KR',
    'PR',
    'RES',
    'pos',
    'posRank',
    'opponent'
]]

# Encode categorical features
X = pd.get_dummies(X, columns = ['pos', 'posRank'])

# Check if there were the correct number of posRanks in the dataset
for rank in ["posRank_RB1", "posRank_RB2", "posRank_RB3"]:
    if rank not in list(X.columns):
        X[rank] = 0

# Make sure we have the necessary columns
X = X[['week', 'age', 'passA_curr', 'passC_curr', 'passY_curr', 'passT_curr', 'passI_curr', 'pass2_curr', 'rushA_curr', 'rushY_curr', 
'rushT_curr', 'rush2_curr', 'recC_curr', 'recY_curr', 'recT_curr', 'rec2_curr', 'fum_curr', 'XPA_curr', 'XPM_curr', 'FGA_curr', 'FGM_curr', 
'FG50_curr', 'defSack_curr', 'defI_curr', 'defSaf_curr', 'defFum_curr', 'defBlk_curr', 'defT_curr', 'defPtsAgainst_curr', 
'defPassYAgainst_curr', 'defRushYAgainst_curr', 'defYdsAgainst_curr', 'gamesPlayed_curr', 'gamesPlayed_prior1', 'passA_prior1', 
'passC_prior1', 'passY_prior1', 'passT_prior1', 'passI_prior1', 'pass2_prior1', 'rushA_prior1', 'rushY_prior1', 'rushT_prior1', 
'rush2_prior1', 'recC_prior1', 'recY_prior1', 'recT_prior1', 'rec2_prior1', 'fum_prior1', 'XPA_prior1', 'XPM_prior1', 'FGA_prior1', 
'FGM_prior1', 'FG50_prior1', 'defSack_prior1', 'defI_prior1', 'defSaf_prior1', 'defFum_prior1', 'defBlk_prior1', 'defT_prior1', 
'defPtsAgainst_prior1', 'defPassYAgainst_prior1', 'defRushYAgainst_prior1', 'defYdsAgainst_prior1', 'gamesPlayed_prior2', 'passA_prior2', 
'passC_prior2', 'passY_prior2', 'passT_prior2', 'passI_prior2', 'pass2_prior2', 'rushA_prior2', 'rushY_prior2', 'rushT_prior2', 
'rush2_prior2', 'recC_prior2', 'recY_prior2', 'recT_prior2', 'rec2_prior2', 'fum_prior2', 'XPA_prior2', 'XPM_prior2', 'FGA_prior2', 
'FGM_prior2', 'FG50_prior2', 'defSack_prior2', 'defI_prior2', 'defSaf_prior2', 'defFum_prior2', 'defBlk_prior2', 'defT_prior2', 
'defPtsAgainst_prior2', 'defPassYAgainst_prior2', 'defRushYAgainst_prior2', 'defYdsAgainst_prior2', 'defSack_curr_opp', 'defI_curr_opp', 
'defSaf_curr_opp', 'defFum_curr_opp', 'defBlk_curr_opp', 'defT_curr_opp', 'defPtsAgainst_curr_opp', 'defPassYAgainst_curr_opp', 
'defRushYAgainst_curr_opp', 'defYdsAgainst_curr_opp', 'defSack_prior1_opp', 'defI_prior1_opp', 'defSaf_prior1_opp', 'defFum_prior1_opp', 
'defBlk_prior1_opp', 'defT_prior1_opp', 'defPtsAgainst_prior1_opp', 'defPassYAgainst_prior1_opp', 'defRushYAgainst_prior1_opp', 
'defYdsAgainst_prior1_opp', 'pos_RB', 'posRank_RB1', 'posRank_RB2', 'posRank_RB3']]

#load saved model
regressor = load('models/rfmodel_RB1.joblib')

# Run model
y_pred = regressor.predict(X)
y_pred = pd.DataFrame(y_pred)
y_pred.columns = labels
y_pred

# Calculate FANTASY scores
# Define scoring multiplier based on league settings
multiplier = [
    0,0,.04,4,-2,2,.1,.1,6,2,.25,.1,6,2,-2,0,1,0,3,5,1,2,2,2,1.5,6,0,0,0,0,1,1
]
# Define bins for defensive PointsAgainst and YardsAgainst based on MFL scoring categories
binList_defPts = [-5,0,6,13,17,21,27,34,45,59,99]
binList_defYds = [0,274,324,375,425,999]
# Define correlating scores for defensive PointsAgainst and YardsAgainst based on league settings
ptList_defPts = [10,8,7,5,3,2,0,-1,-3,-5]
ptList_defYds = [5,2,0,-2,-5]
# Bin and cut the defensive predictions
y_pred['defPtsBin'] = pd.cut(y_pred['defPtsAgainst'], bins=binList_defPts, include_lowest=True, labels=ptList_defPts)
y_pred['defYdsBin'] = pd.cut(y_pred['defYdsAgainst'], bins=binList_defYds, include_lowest=True, labels=ptList_defYds)
# Merge predictions with header columns so we know the players' position
a_pred = header.merge(y_pred, left_index=True, right_index=True)
# Assign value of zero to all non-defensive players' bins
a_pred.loc[a_pred['pos']!='DF', 'defPtsBin'] = 0
a_pred.loc[a_pred['pos']!='DF', 'defYdsBin'] = 0
# Drop the header columns again
a_pred = a_pred.drop(columns=['id_mfl', 'week','season','team','player','age','sharkRank','adp','pos','KR','PR','RES','posRank','opponent'])
# Create function to apply scoring multiplier
def multer(row):
    return row.multiply(multiplier)
# Apply scoring multiplier to predictions
c = a_pred.apply(multer, axis=1)
c = c.apply(np.sum, axis=1)
c = pd.DataFrame(c, columns=['pred'])

# Merge header columns with predictions
RBdf = header.merge(c, left_index=True, right_index=True)

# %%
### QB Predictions
# Read player model and ages
xl2 = player_df.copy()
# Select only one player position
xl2 = xl2.loc[xl2.posRank.isin(['QB1', 'QB2', 'QB3'])]
xl2 = xl2.loc[xl2.pos=='QB']
xl2 = xl2.dropna()
xl2.reset_index(inplace=True, drop=True)

# Select features
X = xl2[features]
header = xl2[[
    'id_mfl',
    'season',
    'week',
    'team',
    'player',
    'age',
    'sharkRank', 
    'adp',
    'KR',
    'PR',
    'RES',
    'pos',
    'posRank',
    'opponent'
]]

# Encode categorical features
X = pd.get_dummies(X, columns = ['pos', 'posRank'])

# Check if there were the correct number of posRanks in the dataset
for rank in ["posRank_QB1", "posRank_QB2", "posRank_QB3"]:
    if rank not in list(X.columns):
        X[rank] = 0

# Make sure we have the necessary columns
X = X[['week', 'age', 'passA_curr', 'passC_curr', 'passY_curr', 'passT_curr', 'passI_curr', 'pass2_curr', 'rushA_curr', 'rushY_curr', 
'rushT_curr', 'rush2_curr', 'recC_curr', 'recY_curr', 'recT_curr', 'rec2_curr', 'fum_curr', 'XPA_curr', 'XPM_curr', 'FGA_curr', 'FGM_curr', 
'FG50_curr', 'defSack_curr', 'defI_curr', 'defSaf_curr', 'defFum_curr', 'defBlk_curr', 'defT_curr', 'defPtsAgainst_curr', 
'defPassYAgainst_curr', 'defRushYAgainst_curr', 'defYdsAgainst_curr', 'gamesPlayed_curr', 'gamesPlayed_prior1', 'passA_prior1', 
'passC_prior1', 'passY_prior1', 'passT_prior1', 'passI_prior1', 'pass2_prior1', 'rushA_prior1', 'rushY_prior1', 'rushT_prior1', 
'rush2_prior1', 'recC_prior1', 'recY_prior1', 'recT_prior1', 'rec2_prior1', 'fum_prior1', 'XPA_prior1', 'XPM_prior1', 'FGA_prior1', 
'FGM_prior1', 'FG50_prior1', 'defSack_prior1', 'defI_prior1', 'defSaf_prior1', 'defFum_prior1', 'defBlk_prior1', 'defT_prior1', 
'defPtsAgainst_prior1', 'defPassYAgainst_prior1', 'defRushYAgainst_prior1', 'defYdsAgainst_prior1', 'gamesPlayed_prior2', 'passA_prior2', 
'passC_prior2', 'passY_prior2', 'passT_prior2', 'passI_prior2', 'pass2_prior2', 'rushA_prior2', 'rushY_prior2', 'rushT_prior2', 
'rush2_prior2', 'recC_prior2', 'recY_prior2', 'recT_prior2', 'rec2_prior2', 'fum_prior2', 'XPA_prior2', 'XPM_prior2', 'FGA_prior2', 
'FGM_prior2', 'FG50_prior2', 'defSack_prior2', 'defI_prior2', 'defSaf_prior2', 'defFum_prior2', 'defBlk_prior2', 'defT_prior2', 
'defPtsAgainst_prior2', 'defPassYAgainst_prior2', 'defRushYAgainst_prior2', 'defYdsAgainst_prior2', 'defSack_curr_opp', 'defI_curr_opp', 
'defSaf_curr_opp', 'defFum_curr_opp', 'defBlk_curr_opp', 'defT_curr_opp', 'defPtsAgainst_curr_opp', 'defPassYAgainst_curr_opp', 
'defRushYAgainst_curr_opp', 'defYdsAgainst_curr_opp', 'defSack_prior1_opp', 'defI_prior1_opp', 'defSaf_prior1_opp', 'defFum_prior1_opp', 
'defBlk_prior1_opp', 'defT_prior1_opp', 'defPtsAgainst_prior1_opp', 'defPassYAgainst_prior1_opp', 'defRushYAgainst_prior1_opp', 
'defYdsAgainst_prior1_opp', 'pos_QB', 'posRank_QB1', 'posRank_QB2', 'posRank_QB3']]

#load saved model
regressor = load('models/rfmodel_QB1.joblib')

# Run model
y_pred = regressor.predict(X)
y_pred = pd.DataFrame(y_pred)
y_pred.columns = labels
y_pred

# Calculate FANTASY scores
# Define scoring multiplier based on league settings
multiplier = [
    0,0,.04,4,-2,2,.1,.1,6,2,.25,.1,6,2,-2,0,1,0,3,5,1,2,2,2,1.5,6,0,0,0,0,1,1
]
# Define bins for defensive PointsAgainst and YardsAgainst based on MFL scoring categories
binList_defPts = [-5,0,6,13,17,21,27,34,45,59,99]
binList_defYds = [0,274,324,375,425,999]
# Define correlating scores for defensive PointsAgainst and YardsAgainst based on league settings
ptList_defPts = [10,8,7,5,3,2,0,-1,-3,-5]
ptList_defYds = [5,2,0,-2,-5]
# Bin and cut the defensive predictions
y_pred['defPtsBin'] = pd.cut(y_pred['defPtsAgainst'], bins=binList_defPts, include_lowest=True, labels=ptList_defPts)
y_pred['defYdsBin'] = pd.cut(y_pred['defYdsAgainst'], bins=binList_defYds, include_lowest=True, labels=ptList_defYds)
# Merge predictions with header columns so we know the players' position
a_pred = header.merge(y_pred, left_index=True, right_index=True)
# Assign value of zero to all non-defensive players' bins
a_pred.loc[a_pred['pos']!='DF', 'defPtsBin'] = 0
a_pred.loc[a_pred['pos']!='DF', 'defYdsBin'] = 0
# Drop the header columns again
a_pred = a_pred.drop(columns=['id_mfl', 'week','season','team','player','age','sharkRank','adp','pos','KR','PR','RES','posRank','opponent'])
# Create function to apply scoring multiplier
def multer(row):
    return row.multiply(multiplier)
# Apply scoring multiplier to predictions
c = a_pred.apply(multer, axis=1)
c = c.apply(np.sum, axis=1)
c = pd.DataFrame(c, columns=['pred'])

# Merge header columns with predictions
QBdf = header.merge(c, left_index=True, right_index=True)

# %%
### TE Predictions
# Read player model and ages
xl2 = player_df.copy()
# Select only one player position
xl2 = xl2.loc[xl2.posRank.isin(['TE1', 'TE2', 'TE3'])]
xl2 = xl2.loc[xl2.pos=='TE']
xl2 = xl2.dropna()
xl2.reset_index(inplace=True, drop=True)

# Select features
X = xl2[features]
header = xl2[[
    'id_mfl',
    'season',
    'week',
    'team',
    'player',
    'age',
    'sharkRank', 
    'adp',
    'KR',
    'PR',
    'RES',
    'pos',
    'posRank',
    'opponent'
]]

# Encode categorical features
X = pd.get_dummies(X, columns = ['pos', 'posRank'])

# Check if there were the correct number of posRanks in the dataset
for rank in ["posRank_TE1", "posRank_TE2", "posRank_TE3"]:
    if rank not in list(X.columns):
        X[rank] = 0

# Make sure we have the necessary columns
X = X[['week', 'age', 'passA_curr', 'passC_curr', 'passY_curr', 'passT_curr', 'passI_curr', 'pass2_curr', 'rushA_curr', 'rushY_curr', 
'rushT_curr', 'rush2_curr', 'recC_curr', 'recY_curr', 'recT_curr', 'rec2_curr', 'fum_curr', 'XPA_curr', 'XPM_curr', 'FGA_curr', 'FGM_curr', 
'FG50_curr', 'defSack_curr', 'defI_curr', 'defSaf_curr', 'defFum_curr', 'defBlk_curr', 'defT_curr', 'defPtsAgainst_curr', 
'defPassYAgainst_curr', 'defRushYAgainst_curr', 'defYdsAgainst_curr', 'gamesPlayed_curr', 'gamesPlayed_prior1', 'passA_prior1', 
'passC_prior1', 'passY_prior1', 'passT_prior1', 'passI_prior1', 'pass2_prior1', 'rushA_prior1', 'rushY_prior1', 'rushT_prior1', 
'rush2_prior1', 'recC_prior1', 'recY_prior1', 'recT_prior1', 'rec2_prior1', 'fum_prior1', 'XPA_prior1', 'XPM_prior1', 'FGA_prior1', 
'FGM_prior1', 'FG50_prior1', 'defSack_prior1', 'defI_prior1', 'defSaf_prior1', 'defFum_prior1', 'defBlk_prior1', 'defT_prior1', 
'defPtsAgainst_prior1', 'defPassYAgainst_prior1', 'defRushYAgainst_prior1', 'defYdsAgainst_prior1', 'gamesPlayed_prior2', 'passA_prior2', 
'passC_prior2', 'passY_prior2', 'passT_prior2', 'passI_prior2', 'pass2_prior2', 'rushA_prior2', 'rushY_prior2', 'rushT_prior2', 
'rush2_prior2', 'recC_prior2', 'recY_prior2', 'recT_prior2', 'rec2_prior2', 'fum_prior2', 'XPA_prior2', 'XPM_prior2', 'FGA_prior2', 
'FGM_prior2', 'FG50_prior2', 'defSack_prior2', 'defI_prior2', 'defSaf_prior2', 'defFum_prior2', 'defBlk_prior2', 'defT_prior2', 
'defPtsAgainst_prior2', 'defPassYAgainst_prior2', 'defRushYAgainst_prior2', 'defYdsAgainst_prior2', 'defSack_curr_opp', 'defI_curr_opp', 
'defSaf_curr_opp', 'defFum_curr_opp', 'defBlk_curr_opp', 'defT_curr_opp', 'defPtsAgainst_curr_opp', 'defPassYAgainst_curr_opp', 
'defRushYAgainst_curr_opp', 'defYdsAgainst_curr_opp', 'defSack_prior1_opp', 'defI_prior1_opp', 'defSaf_prior1_opp', 'defFum_prior1_opp', 
'defBlk_prior1_opp', 'defT_prior1_opp', 'defPtsAgainst_prior1_opp', 'defPassYAgainst_prior1_opp', 'defRushYAgainst_prior1_opp', 
'defYdsAgainst_prior1_opp', 'pos_TE', 'posRank_TE1', 'posRank_TE2', 'posRank_TE3']]

#load saved model
regressor = load('models/rfmodel_TE1.joblib')

# Run model
y_pred = regressor.predict(X)
y_pred = pd.DataFrame(y_pred)
y_pred.columns = labels
y_pred

# Calculate FANTASY scores
# Define scoring multiplier based on league settings
multiplier = [
    0,0,.04,4,-2,2,.1,.1,6,2,.25,.1,6,2,-2,0,1,0,3,5,1,2,2,2,1.5,6,0,0,0,0,1,1
]
# Define bins for defensive PointsAgainst and YardsAgainst based on MFL scoring categories
binList_defPts = [-5,0,6,13,17,21,27,34,45,59,99]
binList_defYds = [0,274,324,375,425,999]
# Define correlating scores for defensive PointsAgainst and YardsAgainst based on league settings
ptList_defPts = [10,8,7,5,3,2,0,-1,-3,-5]
ptList_defYds = [5,2,0,-2,-5]
# Bin and cut the defensive predictions
y_pred['defPtsBin'] = pd.cut(y_pred['defPtsAgainst'], bins=binList_defPts, include_lowest=True, labels=ptList_defPts)
y_pred['defYdsBin'] = pd.cut(y_pred['defYdsAgainst'], bins=binList_defYds, include_lowest=True, labels=ptList_defYds)
# Merge predictions with header columns so we know the players' position
a_pred = header.merge(y_pred, left_index=True, right_index=True)
# Assign value of zero to all non-defensive players' bins
a_pred.loc[a_pred['pos']!='DF', 'defPtsBin'] = 0
a_pred.loc[a_pred['pos']!='DF', 'defYdsBin'] = 0
# Drop the header columns again
a_pred = a_pred.drop(columns=['id_mfl', 'week','season','team','player','age','sharkRank','adp','pos','KR','PR','RES','posRank','opponent'])
# Create function to apply scoring multiplier
def multer(row):
    return row.multiply(multiplier)
# Apply scoring multiplier to predictions
c = a_pred.apply(multer, axis=1)
c = c.apply(np.sum, axis=1)
c = pd.DataFrame(c, columns=['pred'])

# Merge header columns with predictions
TEdf = header.merge(c, left_index=True, right_index=True)

# %%
### PK Predictions
# Read player model and ages
xl2 = player_df.copy()
# Select only one player position
xl2 = xl2.loc[xl2.posRank.isin(['PK1', 'PK2', 'PK3'])]
xl2 = xl2.loc[xl2.pos=='PK']
xl2 = xl2.dropna()
xl2.reset_index(inplace=True, drop=True)

# Select features
X = xl2[features]
header = xl2[[
    'id_mfl',
    'season',
    'week',
    'team',
    'player',
    'age',
    'sharkRank', 
    'adp',
    'KR',
    'PR',
    'RES',
    'pos',
    'posRank',
    'opponent'
]]

# Encode categorical features
X = pd.get_dummies(X, columns = ['pos', 'posRank'])

# Check if there were the correct number of posRanks in the dataset
for rank in ["posRank_PK1", "posRank_PK2", "posRank_PK3"]:
    if rank not in list(X.columns):
        X[rank] = 0

# Make sure we have the necessary columns
X = X[['week', 'age', 'passA_curr', 'passC_curr', 'passY_curr', 'passT_curr', 'passI_curr', 'pass2_curr', 'rushA_curr', 'rushY_curr', 
'rushT_curr', 'rush2_curr', 'recC_curr', 'recY_curr', 'recT_curr', 'rec2_curr', 'fum_curr', 'XPA_curr', 'XPM_curr', 'FGA_curr', 'FGM_curr', 
'FG50_curr', 'defSack_curr', 'defI_curr', 'defSaf_curr', 'defFum_curr', 'defBlk_curr', 'defT_curr', 'defPtsAgainst_curr', 
'defPassYAgainst_curr', 'defRushYAgainst_curr', 'defYdsAgainst_curr', 'gamesPlayed_curr', 'gamesPlayed_prior1', 'passA_prior1', 
'passC_prior1', 'passY_prior1', 'passT_prior1', 'passI_prior1', 'pass2_prior1', 'rushA_prior1', 'rushY_prior1', 'rushT_prior1', 
'rush2_prior1', 'recC_prior1', 'recY_prior1', 'recT_prior1', 'rec2_prior1', 'fum_prior1', 'XPA_prior1', 'XPM_prior1', 'FGA_prior1', 
'FGM_prior1', 'FG50_prior1', 'defSack_prior1', 'defI_prior1', 'defSaf_prior1', 'defFum_prior1', 'defBlk_prior1', 'defT_prior1', 
'defPtsAgainst_prior1', 'defPassYAgainst_prior1', 'defRushYAgainst_prior1', 'defYdsAgainst_prior1', 'gamesPlayed_prior2', 'passA_prior2', 
'passC_prior2', 'passY_prior2', 'passT_prior2', 'passI_prior2', 'pass2_prior2', 'rushA_prior2', 'rushY_prior2', 'rushT_prior2', 
'rush2_prior2', 'recC_prior2', 'recY_prior2', 'recT_prior2', 'rec2_prior2', 'fum_prior2', 'XPA_prior2', 'XPM_prior2', 'FGA_prior2', 
'FGM_prior2', 'FG50_prior2', 'defSack_prior2', 'defI_prior2', 'defSaf_prior2', 'defFum_prior2', 'defBlk_prior2', 'defT_prior2', 
'defPtsAgainst_prior2', 'defPassYAgainst_prior2', 'defRushYAgainst_prior2', 'defYdsAgainst_prior2', 'defSack_curr_opp', 'defI_curr_opp', 
'defSaf_curr_opp', 'defFum_curr_opp', 'defBlk_curr_opp', 'defT_curr_opp', 'defPtsAgainst_curr_opp', 'defPassYAgainst_curr_opp', 
'defRushYAgainst_curr_opp', 'defYdsAgainst_curr_opp', 'defSack_prior1_opp', 'defI_prior1_opp', 'defSaf_prior1_opp', 'defFum_prior1_opp', 
'defBlk_prior1_opp', 'defT_prior1_opp', 'defPtsAgainst_prior1_opp', 'defPassYAgainst_prior1_opp', 'defRushYAgainst_prior1_opp', 
'defYdsAgainst_prior1_opp', 'pos_PK', 'posRank_PK1', 'posRank_PK2', 'posRank_PK3']]

#load saved model
regressor = load('models/rfmodel_PK1.joblib')

# Run model
y_pred = regressor.predict(X)
y_pred = pd.DataFrame(y_pred)
y_pred.columns = labels
y_pred

# Calculate FANTASY scores
# Define scoring multiplier based on league settings
multiplier = [
    0,0,.04,4,-2,2,.1,.1,6,2,.25,.1,6,2,-2,0,1,0,3,5,1,2,2,2,1.5,6,0,0,0,0,1,1
]
# Define bins for defensive PointsAgainst and YardsAgainst based on MFL scoring categories
binList_defPts = [-5,0,6,13,17,21,27,34,45,59,99]
binList_defYds = [0,274,324,375,425,999]
# Define correlating scores for defensive PointsAgainst and YardsAgainst based on league settings
ptList_defPts = [10,8,7,5,3,2,0,-1,-3,-5]
ptList_defYds = [5,2,0,-2,-5]
# Bin and cut the defensive predictions
y_pred['defPtsBin'] = pd.cut(y_pred['defPtsAgainst'], bins=binList_defPts, include_lowest=True, labels=ptList_defPts)
y_pred['defYdsBin'] = pd.cut(y_pred['defYdsAgainst'], bins=binList_defYds, include_lowest=True, labels=ptList_defYds)
# Merge predictions with header columns so we know the players' position
a_pred = header.merge(y_pred, left_index=True, right_index=True)
# Assign value of zero to all non-defensive players' bins
a_pred.loc[a_pred['pos']!='DF', 'defPtsBin'] = 0
a_pred.loc[a_pred['pos']!='DF', 'defYdsBin'] = 0
# Drop the header columns again
a_pred = a_pred.drop(columns=['id_mfl', 'week','season','team','player','age','sharkRank','adp','pos','KR','PR','RES','posRank','opponent'])
# Create function to apply scoring multiplier
def multer(row):
    return row.multiply(multiplier)
# Apply scoring multiplier to predictions
c = a_pred.apply(multer, axis=1)
c = c.apply(np.sum, axis=1)
c = pd.DataFrame(c, columns=['pred'])

# Merge header columns with predictions
PKdf = header.merge(c, left_index=True, right_index=True)

# %%
### DF Predictions
# Read player model and ages
xl2 = player_df.copy()
# Select only one player position
xl2 = xl2.loc[xl2.posRank.isin(['DF1', 'DF2', 'DF3'])]
xl2 = xl2.loc[xl2.pos=='DF']
xl2 = xl2.dropna()
xl2.reset_index(inplace=True, drop=True)

# Select features
X = xl2[features]
header = xl2[[
    'id_mfl',
    'season',
    'week',
    'team',
    'player',
    'age',
    'sharkRank', 
    'adp',
    'KR',
    'PR',
    'RES',
    'pos',
    'posRank',
    'opponent'
]]

# Encode categorical features
X = pd.get_dummies(X, columns = ['pos', 'posRank'])

#load saved model
regressor = load('models/rfmodel_DF1.joblib')

# Run model
y_pred = regressor.predict(X)
y_pred = pd.DataFrame(y_pred)
y_pred.columns = labels
y_pred

# Calculate FANTASY scores
# Define scoring multiplier based on league settings
multiplier = [
    0,0,.04,4,-2,2,.1,.1,6,2,.25,.1,6,2,-2,0,1,0,3,5,1,2,2,2,1.5,6,0,0,0,0,1,1
]
# Define bins for defensive PointsAgainst and YardsAgainst based on MFL scoring categories
binList_defPts = [-5,0,6,13,17,21,27,34,45,59,99]
binList_defYds = [0,274,324,375,425,999]
# Define correlating scores for defensive PointsAgainst and YardsAgainst based on league settings
ptList_defPts = [10,8,7,5,3,2,0,-1,-3,-5]
ptList_defYds = [5,2,0,-2,-5]
# Bin and cut the defensive predictions
y_pred['defPtsBin'] = pd.cut(y_pred['defPtsAgainst'], bins=binList_defPts, include_lowest=True, labels=ptList_defPts)
y_pred['defYdsBin'] = pd.cut(y_pred['defYdsAgainst'], bins=binList_defYds, include_lowest=True, labels=ptList_defYds)
# Merge predictions with header columns so we know the players' position
a_pred = header.merge(y_pred, left_index=True, right_index=True)
# Assign value of zero to all non-defensive players' bins
a_pred.loc[a_pred['pos']!='DF', 'defPtsBin'] = 0
a_pred.loc[a_pred['pos']!='DF', 'defYdsBin'] = 0
# Drop the header columns again
a_pred = a_pred.drop(columns=['id_mfl', 'week','season','team','player','age','sharkRank','adp','pos','KR','PR','RES','posRank','opponent'])
# Create function to apply scoring multiplier
def multer(row):
    return row.multiply(multiplier)
# Apply scoring multiplier to predictions
c = a_pred.apply(multer, axis=1)
c = c.apply(np.sum, axis=1)
c = pd.DataFrame(c, columns=['pred'])

# Merge header columns with predictions
DFdf = header.merge(c, left_index=True, right_index=True)

# %%
# Merge all positions' predictions
complete = pd.concat([WRdf, RBdf, QBdf, TEdf, PKdf, DFdf], axis=0)
complete

# %%
# Create summary of annual scores
# analyze weekly df
tPred = complete.groupby('player')['pred'].sum().to_frame()
tPred.reset_index(inplace=True)
info = complete.drop_duplicates(subset=['player', 'pos'], keep='first')
info = info[[
    'id_mfl', 'player', 'age', 'team', 'pos', 'posRank', 'KR', 'PR', 'RES', 'sharkRank', 'adp'
]]

# Merge all predictions  
fullPred = info.merge(tPred, how='left', left_on='player', right_on='player')
fullPred

# %%
# Convert Shark and ADP to point predictions
# Split player df by player pos
qbs = fullPred[fullPred['pos'] == "QB"]
qbs.reset_index(inplace=True, drop=True)
rbs = fullPred[fullPred['pos'] == "RB"]
rbs.reset_index(inplace=True, drop=True)
wrs = fullPred[fullPred['pos'] == "WR"]
wrs.reset_index(inplace=True, drop=True)
tes = fullPred[fullPred['pos'] == "TE"]
tes.reset_index(inplace=True, drop=True)
pks = fullPred[fullPred['pos'] == "PK"]
pks.reset_index(inplace=True, drop=True)
defs = fullPred[fullPred['pos'] == "DF"]
defs.reset_index(inplace=True, drop=True)

# %%
# Get standard point projections
point_projections = get_df("point_projections")

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

# %%
# Join dfs for current year to point_projection dfs
# Sort current year players by SharkRank
for df in [qbs, rbs, wrs, tes, pks, defs]:
    df.sort_values(by='sharkRank', inplace=True)
    df.reset_index(inplace=True, drop=True)
# Merge dfs
qbs = pd.merge(qbs, qb_proj[['Projection_Relative', 'Projection_Absolute']], how="left", left_index=True, right_index=True)
rbs = pd.merge(rbs, rb_proj[['Projection_Relative', 'Projection_Absolute']], how="left", left_index=True, right_index=True)
wrs = pd.merge(wrs, wr_proj[['Projection_Relative', 'Projection_Absolute']], how="left", left_index=True, right_index=True)
tes = pd.merge(tes, te_proj[['Projection_Relative', 'Projection_Absolute']], how="left", left_index=True, right_index=True)
pks = pd.merge(pks, pk_proj[['Projection_Relative', 'Projection_Absolute']], how="left", left_index=True, right_index=True)
defs = pd.merge(defs, def_proj[['Projection_Relative', 'Projection_Absolute']], how="left", left_index=True, right_index=True)
# Rename columns
for df in [qbs, rbs, wrs, tes, pks, defs]:
    df.rename(columns={'Projection_Relative':'sharkRelative', 'Projection_Absolute':'sharkAbsolute'}, inplace=True)

# %%
# Join dfs for current year to point_projection dfs
# Sort current year players by ADP
for df in [qbs, rbs, wrs, tes, pks, defs]:
    df.sort_values(by='adp', inplace=True)
    df.reset_index(inplace=True, drop=True)
# Merge dfs
qbs = pd.merge(qbs, qb_proj[['Projection_Relative', 'Projection_Absolute']], how="left", left_index=True, right_index=True)
rbs = pd.merge(rbs, rb_proj[['Projection_Relative', 'Projection_Absolute']], how="left", left_index=True, right_index=True)
wrs = pd.merge(wrs, wr_proj[['Projection_Relative', 'Projection_Absolute']], how="left", left_index=True, right_index=True)
tes = pd.merge(tes, te_proj[['Projection_Relative', 'Projection_Absolute']], how="left", left_index=True, right_index=True)
pks = pd.merge(pks, pk_proj[['Projection_Relative', 'Projection_Absolute']], how="left", left_index=True, right_index=True)
defs = pd.merge(defs, def_proj[['Projection_Relative', 'Projection_Absolute']], how="left", left_index=True, right_index=True)
# Rename columns
for df in [qbs, rbs, wrs, tes, pks, defs]:
    df.rename(columns={'Projection_Relative':'adpRelative', 'Projection_Absolute':'adpAbsolute'}, inplace=True)

# %%
# Merge all position dfs into one
predictions = pd.concat([qbs, rbs, wrs, tes, pks, defs])
predictions = predictions.sort_values(by=['pred'], ascending=False)
predictions.reset_index(inplace=True, drop=True)
predictions

# %%
# Account for punt returners and kick returners
predictions.loc[predictions['KR']=='KR1', 'pred'] = predictions.loc[predictions['KR']=='KR1', 'pred'] + 58.5
predictions.loc[predictions['PR']=='PR1', 'pred'] = predictions.loc[predictions['PR']=='PR1', 'pred'] + 25.5
predictions.loc[predictions['KR']=='KR1', 'sharkAbsolute'] = predictions.loc[predictions['KR']=='KR1', 'sharkAbsolute'] + 58.5
predictions.loc[predictions['PR']=='PR1', 'sharkAbsolute'] = predictions.loc[predictions['PR']=='PR1', 'sharkAbsolute'] + 25.5
predictions.loc[predictions['KR']=='KR1', 'adpAbsolute'] = predictions.loc[predictions['KR']=='KR1', 'adpAbsolute'] + 58.5
predictions.loc[predictions['PR']=='PR1', 'adpAbsolute'] = predictions.loc[predictions['PR']=='PR1', 'adpAbsolute'] + 25.5
predictions

# %%
# Send predictions to database
# Prepare predictions df
# Build the SQL query that will list columns and datatypes
string1 = [x + " VARCHAR(32)" for x in predictions.columns[:2]] + [
    x + " SMALLINT" for x in predictions.columns[2:3]] + [
    x + " VARCHAR(32)" for x in predictions.columns[3:9]] + [
    x + " FLOAT(8)" for x in predictions.columns[9:]]
string1 = str(string1)
string1 = string1.replace("'", "")
string1 = string1.replace("[", "")
string1 = string1.replace("]", "")
#print(f'CREATE TABLE IF NOT EXISTS predictions({string1})')


# Write the df to the Postgresql database
try:
    # connect to database
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    engine = create_engine(DATABASE_URL)
    cursor = conn.cursor()
    # Create table for schedule
    cursor.execute(f'CREATE TABLE IF NOT EXISTS predictions({string1})')
    conn.commit()
    # Populate table with data
    predictions.to_sql('predictions', engine, if_exists='replace', index = False)
except Exception as error:
    print(error)
finally:
    if conn:
        cursor.close()
        conn.close()

# %%


# %%



