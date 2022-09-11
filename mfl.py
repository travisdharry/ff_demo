# Import dependencies
# Standard python libraries
import json
import os
from bs4 import BeautifulSoup, ProcessingInstruction
from oauthlib.oauth2 import WebApplicationClient
import requests
import pandas as pd

def get_mfl(requestType, user_league):
    urlString = f"https://www54.myfantasyleague.com/2022/export?TYPE={requestType}&L={user_league}"
    parseDict = {
        "league": {"findRows":"franchise", "findCols":{"id", "name"}, "colNames":{"id":"FranchiseID", "name":"FranchiseName"}},
        "liveScoring": {"findRows":"player", "findCols":{"id", "score", "gameSecondsRemaining", "status"}, "colNames":{"id":"id_mfl", "score":"liveScore"}},
        "rosters": {"findRows":"franchise", "findCols":{"id", "week", "gameSecondsRemaining", "status"}, "colNames":{"id":"id_mfl", "score":"liveScore"}}
        }
    parseBuilder = parseDict.get(requestType)
    # Get xml from MyFantasyLeague
    response = requests.get(urlString)
    soup = BeautifulSoup(response.content,'xml')
    data = []
    # Create df: Get all rows
    df = soup.find_all(parseBuilder.get("findRows"))
    # Get data to fill rows
    for i in range(len(df)):
        # Select columns
        rows = []
        colList = []
        for col in parseBuilder.get("findCols"):
            rows.append(df[i].get(col))
            colList.append(col)
        data.append(rows)
    df = pd.DataFrame(data)
    df.columns = colList
    # Rename columns
    df.rename(columns=parseBuilder.get("colNames"), inplace=True)
    return df

def get_mfl_league(user_league):
    urlString = f"https://www54.myfantasyleague.com/2022/export?TYPE=league&L={user_league}"
    response = requests.get(urlString)
    soup = BeautifulSoup(response.content,'xml')
    data = []
    elems = soup.find_all('franchise')
    for i in range(len(elems)):
        rows = [elems[i].get("id"), elems[i].get("name")]
        data.append(rows)
    df = pd.DataFrame(data)
    df.columns=['franchiseID','franchiseName']
    return df

def get_mfl_liveScoring(user_league):
    urlString = f"https://www54.myfantasyleague.com/2022/export?TYPE=liveScoring&L={user_league}"
    response = requests.get(urlString)
    soup = BeautifulSoup(response.content,'xml')
    data = []
    franchises = soup.find_all('franchise')
    for i in range(0,len(franchises)):
        current_franchise = franchises[i].find_all('player')
        for j in range(0,len(current_franchise)):
            rows = [franchises[i].get("id"), current_franchise[j].get("id"), current_franchise[j].get("score"), current_franchise[j].get("gameSecondsRemaining"), current_franchise[j].get("status")]
            data.append(rows)
    df = pd.DataFrame(data)
    df.columns = ["franchiseID", "id_mfl", "liveScore", "secondsRemaining", "status"]
    return df
