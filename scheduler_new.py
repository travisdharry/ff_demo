### Config
## Import dependencies
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
# Internal imports
from db import get_df
## Find environment variables
DATABASE_URL = os.environ.get("DATABASE_URL", None)
# sqlalchemy deprecated urls which begin with "postgres://"; now it needs to start with "postgresql://"
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)


##########

### Get MyFantasyLeague player data
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

# Merge all dfs from MyFantasyLeague API
player_df = player_df.merge(player_dobs, on='PlayerID', how='left')
player_df = player_df.drop(columns='DOB')
player_df = player_df.merge(shark_df, on='PlayerID', how='left').merge(adp_df, on='PlayerID', how='left')
player_df['SharkRank'].fillna(3000, inplace=True)
player_df['ADP'].fillna(3000, inplace=True)
player_df = player_df.sort_values(by=['SharkRank'])
player_df.reset_index(inplace=True, drop=True)  


### Clean MyFantasyLeague data
## Select only relevant positions
player_df = player_df.loc[player_df['Position'].isin(['QB', 'WR', 'RB', 'TE', 'PK', 'Def'])]
player_df = player_df.reset_index(drop=True)
## Clean Name column
to_join = player_df['Name'].str.split(", ", n=1, expand=True)
to_join.columns = ['lname', 'fname']
to_join['Name'] = to_join['fname'] + " " + to_join['lname']
player_df['Name'] = to_join['Name']
# Change name to Title Case
player_df['Name'] = player_df['Name'].str.upper()
# Drop name punctuation
player_df['Name'] = player_df['Name'].str.replace(".", "")
player_df['Name'] = player_df['Name'].str.replace(",", "")
player_df['Name'] = player_df['Name'].str.replace("'", "")
## Clean position column
player_df['Position'] = player_df['Position'].replace('Def', 'DF')
# Clean Team column
player_df['Team'] = player_df['Team'].replace('FA*', 'FA')
## Change column names
player_df.columns = ['id_mfl', 'player', 'pos_mfl', 'team', 'age']

###############

### Scrape posRanks
# Set Selenium settings
capa = DesiredCapabilities.CHROME
capa["pageLoadStrategy"] = "none"
# Scrape web for stats
url = f"https://www.ourlads.com/nfldepthcharts/depthcharts.aspx"
PATH = "/Applications/chromedriver"
driver = webdriver.Chrome(service=Service(PATH), desired_capabilities=capa)
wait = WebDriverWait(driver, 20)
driver.get(url)
wait.until(EC.presence_of_element_located((By.XPATH, "//table[@id='ctl00_phContent_gvChart']")))
driver.execute_script("window.stop();")
scrape = pd.read_html(driver.find_element(By.XPATH, value="//table[@id='ctl00_phContent_gvChart']").get_attribute("outerHTML"))
scrape = scrape[0]


### Clean scraped posRanks
# Select relevant columns
scrape = scrape[['Team', 'Pos', 'Player 1', 'Player 2','Player 3', 'Player 4', 'Player 5']]
## Transform columns into additional rows
# Select First rank
scrape1 = scrape[['Team', 'Pos', 'Player 1']]
scrape1 = scrape1.rename(columns={'Player 1':'Player'})
scrape1['posRank'] = "1"
# Select Second rank
scrape2 = scrape[['Team', 'Pos', 'Player 2']]
scrape2 = scrape2.rename(columns={'Player 2':'Player'})
scrape2['posRank'] = "2"
# Select Third rank
scrape3 = scrape[['Team', 'Pos', 'Player 3']]
scrape3 = scrape3.rename(columns={'Player 3':'Player'})
scrape3['posRank'] = "3"
# Select Fourth rank
scrape4 = scrape[['Team', 'Pos', 'Player 4']]
scrape4 = scrape4.rename(columns={'Player 4':'Player'})
scrape4['posRank'] = "4"
# Select Fifth rank
scrape5 = scrape[['Team', 'Pos', 'Player 5']]
scrape5 = scrape5.rename(columns={'Player 5':'Player'})
scrape5['posRank'] = "5"
# Concatenate all ranks
scrape_complete = pd.concat([scrape1, scrape2, scrape3, scrape4, scrape5], axis=0, ignore_index=True)
## Clean Position column
# Select only relevant positions
posList = ['LWR', 'RWR', 'SWR', 'TE', 'QB', 'RB', 'PK', 'PR', 'KR', 'RES']
scrape_final = scrape_complete.loc[scrape_complete['Pos'].isin(posList)]
# Convert WR roles to "WR"
scrape_final['Pos'].replace(["LWR", "RWR", "SWR"], "WR", inplace=True)
scrape_final['posRank'] = scrape_final['Pos'] + scrape_final['posRank']
scrape_final = scrape_final.reset_index(drop=True)
scrape_final.dropna(inplace=True)
scrape_final.drop_duplicates(subset=['Player', 'Team', 'Pos'], inplace=True)
## Create columns for KRs and PRs
krs = scrape_final.loc[scrape_final.Pos=='KR']
krs = krs.drop(columns=['Pos'])
krs.columns = ['Team', 'Player', 'KR']
prs = scrape_final.loc[scrape_final.Pos=='PR']
prs = prs.drop(columns=['Pos'])
prs.columns = ['Team', 'Player', 'PR']
## Join pr and pk scrapes back onto main scrape
scrape_final = scrape_final.merge(krs, how='left', on=['Player', 'Team']).merge(prs, how='left', on=['Player', 'Team'])
scrape_final['KR'].fillna("NO", inplace=True)
scrape_final['PR'].fillna("NO", inplace=True)
## Clean name column
# Remove draft/trade info from scrape
names = scrape_final['Player'].str.split(" ", n=2, expand=True)
# Reorder from "Lname, Fname" to "FName LName"
names.columns = ['a', 'b', 'c']
names['a'] = names['a'].str.replace(",", "")
scrape_final['Player'] = names['b'] + " " + names['a']
# Change to Upper Case
scrape_final['Player'] = scrape_final['Player'].str.upper()
# Drop punctuation
scrape_final['Player'] = scrape_final['Player'].str.replace(".", "")
scrape_final['Player'] = scrape_final['Player'].str.replace(",", "")
scrape_final['Player'] = scrape_final['Player'].str.replace("'", "")
## Change column names and order
scrape_final = scrape_final[['Player', 'Pos', 'Team', 'posRank', 'KR', 'PR']]
scrape_final.columns = ['player', 'pos_ol', 'team', 'posRank', 'KR', 'PR']
## Remove separate rows for PRs and KRs
scrape_final = scrape_final.loc[(scrape_final.pos_ol!="KR")]
scrape_final = scrape_final.loc[(scrape_final.pos_ol!="PR")]
## Drop position column
scrape_final.drop(columns=['pos_ol'], inplace=True)


###############

### Merge MyFantasyLeague data with scraped data
player_df = player_df.merge(scrape_final, how='left', on=['player', 'team'])
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


###############





