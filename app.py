# Import dependencies
# Standard python libraries
import json
import os
# Third-party libraries
from flask import Flask, redirect, request, url_for, render_template, session
from flask_login import (
    UserMixin,
    LoginManager,
    current_user,
    login_required,
    login_user,
    logout_user,
)
from bs4 import BeautifulSoup, ProcessingInstruction
from oauthlib.oauth2 import WebApplicationClient
import requests
import pandas as pd
import psycopg2
import plotly
import plotly.express as px
import plotly.graph_objects as go

# Internal imports
from user import User
from db import get_df
from mfl import get_mfl, get_mfl_liveScoring, get_mfl_league

# Configuration (These variables are stored as environment variables)
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", None)
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", None)
GOOGLE_DISCOVERY_URL = ("https://accounts.google.com/.well-known/openid-configuration")

# Find environment variables
DATABASE_URL = os.environ.get("DATABASE_URL", None)
# sqlalchemy deprecated urls which begin with "postgres://"; now it needs to start with "postgresql://"
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Create a new Flask instance
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY")

# Log in users
# User session management setup using Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
# OAuth 2 client setup
client = WebApplicationClient(GOOGLE_CLIENT_ID)
# function to get provider configuration, which will tell us the authorization endpoint
def get_google_provider_cfg():
    return requests.get(GOOGLE_DISCOVERY_URL).json()
# Flask-Login helper to retrieve a user from our db
@login_manager.user_loader
def load_user(user_id):
    return User.get(user_id)


# Create Flask route
@app.route('/')
def index():
    if current_user.is_authenticated:
        return render_template("getLeague.html", user_id=current_user.id)
    else:
        return render_template("index.html")

# If user is not already logged in they are redirected here
@app.route("/login")
def login():
    # Find out what URL to hit for Google login
    google_provider_cfg = get_google_provider_cfg()
    authorization_endpoint = google_provider_cfg["authorization_endpoint"]
    # Use library to construct the request for Google login and provide
    # scopes that let you retrieve user's profile from Google
    request_uri = client.prepare_request_uri(
        authorization_endpoint,
        redirect_uri=request.base_url + "/callback",
        scope=["openid", "email", "profile"],
    )
    return redirect(request_uri)
# When Google has logged the user in the information is sent here:
@app.route("/login/callback")
def callback():
    # Get authorization code Google sent back to you
    code = request.args.get("code")
    # Find out what URL to hit to get tokens that allow you to ask for things on behalf of a user
    google_provider_cfg = get_google_provider_cfg()
    token_endpoint = google_provider_cfg["token_endpoint"]
    # Prepare and send a request to get tokens
    token_url, headers, body = client.prepare_token_request(
        token_endpoint,
        authorization_response=request.url,
        redirect_url=request.base_url,
        code=code
    )
    token_response = requests.post(
        token_url,
        headers=headers,
        data=body,
        auth=(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET),
    )
    # Parse the tokens
    client.parse_request_body_response(json.dumps(token_response.json()))
    # Find and hit the URL from Google that gives the user's profile information,
    userinfo_endpoint = google_provider_cfg["userinfo_endpoint"]
    uri, headers, body = client.add_token(userinfo_endpoint)
    userinfo_response = requests.get(uri, headers=headers, data=body)
    # Make sure their email is verified.
    # The user authenticated with Google, authorized your app, and now you've verified their email through Google!
    if userinfo_response.json().get("email_verified"):
        unique_id = userinfo_response.json()["sub"]
        users_email = userinfo_response.json()["email"]
        users_name = userinfo_response.json()["given_name"]
    else:
        return "User email not available or not verified by Google.", 400
    
    # Create a user with the information provided by Google
    user = User(
        id_=users_email
    )

    if not User.get(users_email):
        # Send user to failure page
        return '<p>"Login failure"</p>'
    else: 
        # Begin user session by logging the user in
        login_user(user)
        # Send user back to index page
        return redirect(url_for("index"))

@app.route('/getLeague')
#@login_required
def getLeague():
    return render_template("getLeague.html")

@app.route("/getLeague/leagueCallback", methods=['GET', 'POST'])
#@login_required
def leagueCallback():
    user_league = request.form["user_league"]
    session['user_league'] = user_league
    return redirect(url_for("getFranchise"))

@app.route('/getFranchise', methods=['GET', 'POST'])
#@login_required
def getFranchise():
    user_league = session.get("user_league")

    # Get Franchises in the league
    urlString = f"https://www54.myfantasyleague.com/2022/export?TYPE=league&L={user_league}"
    response = requests.get(urlString)
    soup = BeautifulSoup(response.content,'xml')
    data = []
    franchises = soup.find_all('franchise')
    for i in range(len(franchises)):
        rows = [franchises[i].get("id"), franchises[i].get("name")]
        data.append(rows)

    return render_template("getFranchise.html", franchise_list=data)

@app.route("/getFranchise/franchiseCallback", methods=['GET', 'POST'])
#@login_required
def franchiseCallback():
    user_franchise = request.form["FranchiseName"]
    session['user_franchise'] = user_franchise
    return redirect(url_for("landing"))

@app.route('/landing')
#@login_required
def landing():
    user_franchise = session.get('user_franchise', None)
    user_league = session.get('user_league', None)
    return render_template("landing.html", user_league=user_league, user_franchise=user_franchise)

@app.route("/allPlayers")
#@login_required
def allPlayers():
    player_df = get_df("player_df")
    return render_template("allPlayers.html", tables=[player_df.to_html(classes='data')], titles=player_df.columns.values)

@app.route('/compareFranchises')
#@login_required
def compareFranchises():
    user_league = session.get("user_league")

    # Get Franchises in the league
    urlString = f"https://www54.myfantasyleague.com/2022/export?TYPE=league&L={user_league}"
    response = requests.get(urlString)
    soup = BeautifulSoup(response.content,'xml')
    data = []
    franchises = soup.find_all('franchise')
    for i in range(len(franchises)):
        rows = [franchises[i].get("id"), franchises[i].get("name")]
        data.append(rows)
    franchise_df = pd.DataFrame(data)
    franchise_df.columns=['FranchiseID','FranchiseName']
    franchise_df = franchise_df.append({"FranchiseID":"FA", "FranchiseName":"Free Agent"}, ignore_index=True)

    # Get franchise rosters
    urlString = f"https://www54.myfantasyleague.com/2022/export?TYPE=rosters&L={user_league}"
    response = requests.get(urlString)
    soup = BeautifulSoup(response.content,'xml')
    data = []
    franchises = soup.find_all('franchise')
    for i in range(0,len(franchises)):
        current_franchise = franchises[i].find_all('player')
        for j in range(0,len(current_franchise)):
            rows = [franchises[i].get("id"), franchises[i].get("week"), current_franchise[j].get("id"), current_franchise[j].get("status")]
            data.append(rows)
    rosters_df = pd.DataFrame(data)

    # Get Free Agents
    urlString = f"https://www54.myfantasyleague.com/2022/export?TYPE=freeAgents&L={user_league}"
    response = requests.get(urlString)
    soup = BeautifulSoup(response.content,'xml')
    data = []
    freeAgents = soup.find_all('player')
    for i in range(len(freeAgents)):
        rows = ["FA", "", freeAgents[i].get("id"), "Free Agent"]
        data.append(rows)
    fa_df = pd.DataFrame(data)
    rosters_df = rosters_df.append(fa_df)
    rosters_df.columns=['FranchiseID','Week','PlayerID','RosterStatus']

    # Get all players, sharkRank, and ADP
    player_df = get_df("player_df")

    # Merge all dfs
    complete = player_df.merge(rosters_df, on='PlayerID', how='left').merge(franchise_df[['FranchiseID', 'FranchiseName']], on='FranchiseID', how='left')
    complete['FranchiseID'].fillna("FA", inplace=True)
    complete['FranchiseName'].fillna("Free Agent", inplace=True)
    complete['RosterStatus'].fillna("Free Agent", inplace=True)
    complete = complete.sort_values(by=['SharkRank'])
    complete.reset_index(inplace=True, drop=True)

    # Split complete df by player position
    qbs = complete[complete['Position'] == "QB"]
    qbs.reset_index(inplace=True, drop=True)
    rbs = complete[complete['Position'] == "RB"]
    rbs.reset_index(inplace=True, drop=True)
    wrs = complete[complete['Position'] == "WR"]
    wrs.reset_index(inplace=True, drop=True)
    tes = complete[complete['Position'] == "TE"]
    tes.reset_index(inplace=True, drop=True)
    pks = complete[complete['Position'] == "PK"]
    pks.reset_index(inplace=True, drop=True)
    defs = complete[complete['Position'] == "Def"]
    defs.reset_index(inplace=True, drop=True)

    # Roster Builder logic
    qbs_top = qbs.sort_values(by='Projection_Relative', ascending=False, ignore_index=True).groupby('FranchiseName').head(1)
    rbs_top = rbs.sort_values(by='Projection_Relative', ascending=False, ignore_index=True).groupby('FranchiseName').head(2)
    wrs_top = wrs.sort_values(by='Projection_Relative', ascending=False, ignore_index=True).groupby('FranchiseName').head(3)
    tes_top = tes.sort_values(by='Projection_Relative', ascending=False, ignore_index=True).groupby('FranchiseName').head(2)

    qbs_remainder = qbs[~qbs['PlayerID'].isin(qbs_top['PlayerID'])].groupby('FranchiseName').head(1)
    rbs_remainder = rbs[~rbs['PlayerID'].isin(rbs_top['PlayerID'])].groupby('FranchiseName').head(3)
    wrs_remainder = wrs[~wrs['PlayerID'].isin(wrs_top['PlayerID'])].groupby('FranchiseName').head(3)
    tes_remainder = tes[~tes['PlayerID'].isin(tes_top['PlayerID'])].groupby('FranchiseName').head(3)

    remainder = pd.concat([qbs_remainder, rbs_remainder, wrs_remainder, tes_remainder])

    top_remainders = remainder.sort_values(by='Projection_Absolute', ascending=False, ignore_index=True).groupby('FranchiseName').head(3)

    players_onthefield = pd.concat([qbs_top, rbs_top, wrs_top, tes_top, top_remainders])
    players_onthefield = players_onthefield.sort_values(by='Projection_Absolute', ascending=False, ignore_index=True)

    fran_rank = players_onthefield.groupby('FranchiseName').sum().sort_values(by='Projection_Absolute', ascending=False)

    sorter = fran_rank.index

    players_onthefield.FranchiseName = players_onthefield.FranchiseName.astype("category")
    players_onthefield.FranchiseName.cat.set_categories(sorter, inplace=True)
    players_onthefield.sort_values(["FranchiseName"], inplace=True)

    # Create bar chart
    fig = px.bar(players_onthefield, 
                x="FranchiseName", 
                y="Projection_Relative", 
                color="Position", 
                text='Name', 
                color_discrete_map={
                    "RB": "#062647", #blue #1033a6 #0c2987 1033a6 062647
                    "TE": "#43B3AE", #teal #02687b #038097 1295ad 43B3AE
                    "WR": "#621B74", #purple #4f22bc #643fc1 643fc1 621B74
                    "QB": "#ffa524"}, #gold #f5d000 f5d000 ffa524
                category_orders={
                    "Position": ["RB", "QB", "WR", "TE"]}
                )
    fig.update_layout(barmode='stack', xaxis={'categoryorder':'total descending'})

    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('compareFranchises.html', graphJSON=graphJSON)

@app.route('/waiverWire', methods=['GET', 'POST'])
#@login_required
def waiverWire():
    user_league = session.get('user_league', None)
    user_franchise = session.get('user_franchise', None)

    # Get Franchises in the league
    urlString = f"https://www54.myfantasyleague.com/2022/export?TYPE=league&L={user_league}"
    response = requests.get(urlString)
    soup = BeautifulSoup(response.content,'xml')
    data = []
    franchises = soup.find_all('franchise')
    for i in range(len(franchises)):
        rows = [franchises[i].get("id"), franchises[i].get("name")]
        data.append(rows)
    franchise_df = pd.DataFrame(data)
    franchise_df.columns=['FranchiseID','FranchiseName']
    franchise_df = franchise_df.append({"FranchiseID":"FA", "FranchiseName":"Free Agent"}, ignore_index=True)

    # Get franchise rosters
    urlString = f"https://www54.myfantasyleague.com/2022/export?TYPE=rosters&L={user_league}&FRANCHISE={user_franchise}"
    response = requests.get(urlString)
    soup = BeautifulSoup(response.content,'xml')
    data = []
    franchises = soup.find_all('franchise')
    for i in range(0,len(franchises)):
        current_franchise = franchises[i].find_all('player')
        for j in range(0,len(current_franchise)):
            rows = [franchises[i].get("id"), franchises[i].get("week"), current_franchise[j].get("id"), current_franchise[j].get("status")]
            data.append(rows)
    rosters_df = pd.DataFrame(data)

    # Get Free Agents
    urlString = f"https://www54.myfantasyleague.com/2022/export?TYPE=freeAgents&L={user_league}"
    response = requests.get(urlString)
    soup = BeautifulSoup(response.content,'xml')
    data = []
    freeAgents = soup.find_all('player')
    for i in range(len(freeAgents)):
        rows = ["FA", "", freeAgents[i].get("id"), "Free Agent"]
        data.append(rows)
    fa_df = pd.DataFrame(data)
    rosters_df = rosters_df.append(fa_df)
    rosters_df.columns=['FranchiseID','Week','PlayerID','RosterStatus']

    # Get all players, sharkRank, and ADP
    player_df = get_df("predictions")

    # Merge all dfs
    complete = player_df.merge(rosters_df, left_on='id_mfl', how='left', right_on='PlayerID').merge(franchise_df[['FranchiseID', 'FranchiseName']], on='FranchiseID', how='left')
    complete = complete[complete['FranchiseID'].notna()]
    complete = complete.sort_values(by=['pred'], ascending=False)
    complete.reset_index(inplace=True, drop=True)
    complete = complete[['player', 'age', 'team', 'FranchiseName', 'pos', 'posRank', 'KR', 'PR', 'RES', 'pred', 'sharkAbsolute', 'adpAbsolute']]
    complete.rename(columns={
        'player':'Player',
        'age':'Age',
        'team':'Team',
        'pos':'Position',
        'posRank': 'Rank',
        'pred': 'ChopBlock Prediction',
        'sharkAbsolute': 'FantasySharks Prediction',
        'adpAbsolute': 'ADP-Based Prediction'
    }, inplace=True)
    complete.set_index('Player', drop=True, inplace=True)

    return render_template("waiverWire.html", tables=[complete.to_html(classes='data')], titles=complete.columns.values)


@app.route('/compareFranchises2')
#@login_required
def compareFranchises2():
    user_league = session.get("user_league")

    # Get Franchises in the league
    urlString = f"https://www54.myfantasyleague.com/2022/export?TYPE=league&L={user_league}"
    response = requests.get(urlString)
    soup = BeautifulSoup(response.content,'xml')
    data = []
    franchises = soup.find_all('franchise')
    for i in range(len(franchises)):
        rows = [franchises[i].get("id"), franchises[i].get("name")]
        data.append(rows)
    franchise_df = pd.DataFrame(data)
    franchise_df.columns=['FranchiseID','FranchiseName']
    franchise_df = franchise_df.append({"FranchiseID":"FA", "FranchiseName":"Free Agent"}, ignore_index=True)

    # Get franchise rosters
    urlString = f"https://www54.myfantasyleague.com/2022/export?TYPE=rosters&L={user_league}"
    response = requests.get(urlString)
    soup = BeautifulSoup(response.content,'xml')
    data = []
    franchises = soup.find_all('franchise')
    for i in range(0,len(franchises)):
        current_franchise = franchises[i].find_all('player')
        for j in range(0,len(current_franchise)):
            rows = [franchises[i].get("id"), franchises[i].get("week"), current_franchise[j].get("id"), current_franchise[j].get("status")]
            data.append(rows)
    rosters_df = pd.DataFrame(data)

    # Get Free Agents
    urlString = f"https://www54.myfantasyleague.com/2022/export?TYPE=freeAgents&L={user_league}"
    response = requests.get(urlString)
    soup = BeautifulSoup(response.content,'xml')
    data = []
    freeAgents = soup.find_all('player')
    for i in range(len(freeAgents)):
        rows = ["FA", "", freeAgents[i].get("id"), "Free Agent"]
        data.append(rows)
    fa_df = pd.DataFrame(data)
    rosters_df = rosters_df.append(fa_df)
    rosters_df.columns=['FranchiseID','Week','PlayerID','RosterStatus']

    # Get all players, sharkRank, and ADP
    predictions = get_df("predictions")

    # Merge all dfs
    complete = predictions.merge(rosters_df, left_on='id_mfl', right_on='PlayerID', how='left').merge(franchise_df[['FranchiseID', 'FranchiseName']], on='FranchiseID', how='left')
    complete['FranchiseID'].fillna("FA", inplace=True)
    complete['FranchiseName'].fillna("Free Agent", inplace=True)
    complete['RosterStatus'].fillna("Free Agent", inplace=True)

    # Split complete df by player pos
    qbs = complete[complete['pos'] == "QB"]
    qbs.reset_index(inplace=True, drop=True)
    rbs = complete[complete['pos'] == "RB"]
    rbs.reset_index(inplace=True, drop=True)
    wrs = complete[complete['pos'] == "WR"]
    wrs.reset_index(inplace=True, drop=True)
    tes = complete[complete['pos'] == "TE"]
    tes.reset_index(inplace=True, drop=True)
    pks = complete[complete['pos'] == "PK"]
    pks.reset_index(inplace=True, drop=True)
    defs = complete[complete['pos'] == "DF"]
    defs.reset_index(inplace=True, drop=True)


    ### ADP Predictions
    # Roster Builder logic
    qbs_top = qbs.sort_values(by='adpAbsolute', ascending=False, ignore_index=True).groupby('FranchiseName').head(1)
    rbs_top = rbs.sort_values(by='adpAbsolute', ascending=False, ignore_index=True).groupby('FranchiseName').head(2)
    wrs_top = wrs.sort_values(by='adpAbsolute', ascending=False, ignore_index=True).groupby('FranchiseName').head(3)
    tes_top = tes.sort_values(by='adpAbsolute', ascending=False, ignore_index=True).groupby('FranchiseName').head(2)
    pks_top = pks.sort_values(by='adpAbsolute', ascending=False, ignore_index=True).groupby('FranchiseName').head(2)
    defs_top = defs.sort_values(by='adpAbsolute', ascending=False, ignore_index=True).groupby('FranchiseName').head(2)

    qbs_remainder = qbs[~qbs['PlayerID'].isin(qbs_top['PlayerID'])].groupby('FranchiseName').head(1)
    rbs_remainder = rbs[~rbs['PlayerID'].isin(rbs_top['PlayerID'])].groupby('FranchiseName').head(3)
    wrs_remainder = wrs[~wrs['PlayerID'].isin(wrs_top['PlayerID'])].groupby('FranchiseName').head(3)
    tes_remainder = tes[~tes['PlayerID'].isin(tes_top['PlayerID'])].groupby('FranchiseName').head(3)

    remainder = pd.concat([qbs_remainder, rbs_remainder, wrs_remainder, tes_remainder])

    top_remainders = remainder.sort_values(by='adpAbsolute', ascending=False, ignore_index=True).groupby('FranchiseName').head(3)

    players_onthefield = pd.concat([qbs_top, rbs_top, wrs_top, tes_top, pks_top, defs_top, top_remainders])
    players_onthefield = players_onthefield.sort_values(by='adpAbsolute', ascending=False, ignore_index=True)

    fran_rank = players_onthefield.groupby('FranchiseName').sum().sort_values(by='adpAbsolute', ascending=False)

    sorter = fran_rank.index

    players_onthefield.FranchiseName = players_onthefield.FranchiseName.astype("category")
    players_onthefield.FranchiseName.cat.set_categories(sorter, inplace=True)
    players_onthefield.sort_values(["FranchiseName"], inplace=True)

    # remove Free Agents
    players_onthefield = players_onthefield.loc[players_onthefield.FranchiseID!="FA"]

    # Find the lowest scoring player on the field and set them as the low bar
    for x in ["QB", "RB", "WR", "TE", "PK", "DF"]:
        players_onthefield.loc[players_onthefield['pos']==x, 'adpComp'] = players_onthefield.loc[players_onthefield['pos']==x, 'adpAbsolute'].min()
    players_onthefield['adpRelative'] = players_onthefield['adpAbsolute'] - players_onthefield['adpComp']

    # Create bar chart
    figADP = px.bar(players_onthefield, 
                x="FranchiseName", 
                y="adpRelative", 
                color="pos", 
                text='player', 
                color_discrete_map={
                    "QB": "hsla(210, 60%, 25%, 1)", #blue #1033a6 #0c2987 1033a6 062647 #293745 
                    "RB": "hsla(12, 50%, 45%, 1)", #gold #f5d000 ffa524 a23419 a34e39
                    "WR": "hsla(267, 40%, 45%, 1)", #purple #4f22bc #643fc1 643fc1 621B74 675280
                    "TE": "hsla(177, 68%, 36%, 1)", #teal #02687b #038097 1295ad 43B3AE
                    "PK": "hsla(14, 30%, 40%, 1)", #gold #f5d000 ffa524 664e47
                    "DF": "hsla(35, 70%, 65%, 1)"}, #gold #f5d000 ffa524 a49375 ffb54d
                category_orders={
                    "pos": ["QB", "RB", "WR", "TE", "PK", "DF"]},
                hover_name="player",
                hover_data={
                    'adpRelative':True, 'pred':True, 'sharkAbsolute':True, 'adpAbsolute':True,
                    'player':False, 'pos':False, 'FranchiseName':False
                    },
                labels={
                    "FranchiseName":"Franchise",
                    "adpRelative":"Player Value",
                    "pred":"ChopBlock Prediction",
                    "sharkAbsolute":"FantasySharks Prediction",
                    "adpAbsolute":"ADP-Based Prediction"
                }
                )
    figADP.update_layout(
                barmode='stack', 
                xaxis={'categoryorder':'total descending'},
                plot_bgcolor='rgba(0,0,0,0)',
                title="ADP-Based Predictions",
                font_family="Skia",
                showlegend=False
                )

    graphJSON_adp = json.dumps(figADP, cls=plotly.utils.PlotlyJSONEncoder)


    ### Shark Predictions
    # Roster Builder logic
    qbs_top = qbs.sort_values(by='sharkAbsolute', ascending=False, ignore_index=True).groupby('FranchiseName').head(1)
    rbs_top = rbs.sort_values(by='sharkAbsolute', ascending=False, ignore_index=True).groupby('FranchiseName').head(2)
    wrs_top = wrs.sort_values(by='sharkAbsolute', ascending=False, ignore_index=True).groupby('FranchiseName').head(3)
    tes_top = tes.sort_values(by='sharkAbsolute', ascending=False, ignore_index=True).groupby('FranchiseName').head(2)
    pks_top = pks.sort_values(by='sharkAbsolute', ascending=False, ignore_index=True).groupby('FranchiseName').head(2)
    defs_top = defs.sort_values(by='sharkAbsolute', ascending=False, ignore_index=True).groupby('FranchiseName').head(2)

    qbs_remainder = qbs[~qbs['PlayerID'].isin(qbs_top['PlayerID'])].groupby('FranchiseName').head(1)
    rbs_remainder = rbs[~rbs['PlayerID'].isin(rbs_top['PlayerID'])].groupby('FranchiseName').head(3)
    wrs_remainder = wrs[~wrs['PlayerID'].isin(wrs_top['PlayerID'])].groupby('FranchiseName').head(3)
    tes_remainder = tes[~tes['PlayerID'].isin(tes_top['PlayerID'])].groupby('FranchiseName').head(3)

    remainder = pd.concat([qbs_remainder, rbs_remainder, wrs_remainder, tes_remainder])

    top_remainders = remainder.sort_values(by='sharkAbsolute', ascending=False, ignore_index=True).groupby('FranchiseName').head(3)

    players_onthefield = pd.concat([qbs_top, rbs_top, wrs_top, tes_top, pks_top, defs_top, top_remainders])
    players_onthefield = players_onthefield.sort_values(by='sharkAbsolute', ascending=False, ignore_index=True)

    fran_rank = players_onthefield.groupby('FranchiseName').sum().sort_values(by='sharkAbsolute', ascending=False)

    sorter = fran_rank.index

    players_onthefield.FranchiseName = players_onthefield.FranchiseName.astype("category")
    players_onthefield.FranchiseName.cat.set_categories(sorter, inplace=True)
    players_onthefield.sort_values(["FranchiseName"], inplace=True)

    # remove Free Agents
    players_onthefield = players_onthefield.loc[players_onthefield.FranchiseID!="FA"]

    # Find the lowest scoring player on the field and set them as the low bar
    for x in ["QB", "RB", "WR", "TE", "PK", "DF"]:
        players_onthefield.loc[players_onthefield['pos']==x, 'sharkComp'] = players_onthefield.loc[players_onthefield['pos']==x, 'sharkAbsolute'].min()
    players_onthefield['sharkRelative'] = players_onthefield['sharkAbsolute'] - players_onthefield['sharkComp']

    # Create bar chart
    figShark = px.bar(players_onthefield, 
                x="FranchiseName", 
                y="sharkRelative", 
                color="pos", 
                text='player', 
                color_discrete_map={
                    "QB": "hsla(210, 60%, 25%, 1)", #blue #1033a6 #0c2987 1033a6 062647 #293745 
                    "RB": "hsla(12, 50%, 45%, 1)", #gold #f5d000 ffa524 a23419 a34e39
                    "WR": "hsla(267, 40%, 45%, 1)", #purple #4f22bc #643fc1 643fc1 621B74 675280
                    "TE": "hsla(177, 68%, 36%, 1)", #teal #02687b #038097 1295ad 43B3AE
                    "PK": "hsla(14, 30%, 40%, 1)", #gold #f5d000 ffa524 664e47
                    "DF": "hsla(35, 70%, 65%, 1)"}, #gold #f5d000 ffa524 a49375 ffb54d
                category_orders={
                    "pos": ["QB", "RB", "WR", "TE", "PK", "DF"]},
                hover_name="player",
                hover_data={
                    'sharkRelative':True, 'pred':True, 'sharkAbsolute':True, 'adpAbsolute':True,
                    'player':False, 'pos':False, 'FranchiseName':False
                    },
                labels={
                    "FranchiseName":"Franchise",
                    "sharkRelative":"Player Value",
                    "pred":"ChopBlock Prediction",
                    "sharkAbsolute":"FantasySharks Prediction",
                    "adpAbsolute":"ADP-Based Prediction"
                }
                )
    figShark.update_layout(
                barmode='stack', 
                xaxis={'categoryorder':'total descending'},
                plot_bgcolor='rgba(0,0,0,0)',
                title="FantasySharks Predictions",
                font_family="Skia",
                showlegend=False
                )

    graphJSON_shark = json.dumps(figShark, cls=plotly.utils.PlotlyJSONEncoder)



    ### My predictions
    # Roster Builder logic
    qbs_top = qbs.sort_values(by='pred', ascending=False, ignore_index=True).groupby('FranchiseName').head(1)
    rbs_top = rbs.sort_values(by='pred', ascending=False, ignore_index=True).groupby('FranchiseName').head(2)
    wrs_top = wrs.sort_values(by='pred', ascending=False, ignore_index=True).groupby('FranchiseName').head(3)
    tes_top = tes.sort_values(by='pred', ascending=False, ignore_index=True).groupby('FranchiseName').head(2)
    pks_top = pks.sort_values(by='pred', ascending=False, ignore_index=True).groupby('FranchiseName').head(2)
    defs_top = defs.sort_values(by='pred', ascending=False, ignore_index=True).groupby('FranchiseName').head(2)

    qbs_remainder = qbs[~qbs['PlayerID'].isin(qbs_top['PlayerID'])].groupby('FranchiseName').head(1)
    rbs_remainder = rbs[~rbs['PlayerID'].isin(rbs_top['PlayerID'])].groupby('FranchiseName').head(3)
    wrs_remainder = wrs[~wrs['PlayerID'].isin(wrs_top['PlayerID'])].groupby('FranchiseName').head(3)
    tes_remainder = tes[~tes['PlayerID'].isin(tes_top['PlayerID'])].groupby('FranchiseName').head(3)

    remainder = pd.concat([qbs_remainder, rbs_remainder, wrs_remainder, tes_remainder])

    top_remainders = remainder.sort_values(by='pred', ascending=False, ignore_index=True).groupby('FranchiseName').head(3)

    players_onthefield = pd.concat([qbs_top, rbs_top, wrs_top, tes_top, pks_top, defs_top, top_remainders])
    players_onthefield = players_onthefield.sort_values(by='pred', ascending=False, ignore_index=True)

    fran_rank = players_onthefield.groupby('FranchiseName').sum().sort_values(by='pred', ascending=False)

    sorter = fran_rank.index

    players_onthefield.FranchiseName = players_onthefield.FranchiseName.astype("category")
    players_onthefield.FranchiseName.cat.set_categories(sorter, inplace=True)
    players_onthefield.sort_values(["FranchiseName"], inplace=True)

    # remove Free Agents
    players_onthefield = players_onthefield.loc[players_onthefield.FranchiseID!="FA"]

    # Find the lowest scoring player on the field and set them as the low bar
    for x in ["QB", "RB", "WR", "TE", "PK", "DF"]:
        players_onthefield.loc[players_onthefield['pos']==x, 'predComp'] = players_onthefield.loc[players_onthefield['pos']==x, 'pred'].min()
    players_onthefield['predRelative'] = players_onthefield['pred'] - players_onthefield['predComp']

    # Create bar chart
    figPred = px.bar(players_onthefield, 
                x="FranchiseName", 
                y="predRelative", 
                color="pos", 
                text='player', 
                color_discrete_map={
                    "QB": "hsla(210, 60%, 25%, 1)", #blue #1033a6 #0c2987 1033a6 062647 #293745 
                    "RB": "hsla(12, 50%, 45%, 1)", #gold #f5d000 ffa524 a23419 a34e39
                    "WR": "hsla(267, 40%, 45%, 1)", #purple #4f22bc #643fc1 643fc1 621B74 675280
                    "TE": "hsla(177, 68%, 36%, 1)", #teal #02687b #038097 1295ad 43B3AE
                    "PK": "hsla(14, 30%, 40%, 1)", #gold #f5d000 ffa524 664e47
                    "DF": "hsla(35, 70%, 65%, 1)"}, #gold #f5d000 ffa524 a49375 ffb54d
                category_orders={
                    "pos": ["QB", "RB", "WR", "TE", "PK", "DF"]},
                hover_name="player",
                hover_data={
                    'predRelative':True, 'pred':True, 'sharkAbsolute':True, 'adpAbsolute':True,
                    'player':False, 'pos':False, 'FranchiseName':False
                    },
                labels={
                    "FranchiseName":"Franchise",
                    "predRelative":"Player Value",
                    "pred":"ChopBlock Prediction",
                    "sharkAbsolute":"FantasySharks Prediction",
                    "adpAbsolute":"ADP-Based Prediction"
                }
                )
    figPred.update_layout(
                barmode='stack', 
                xaxis={'categoryorder':'total descending'},
                plot_bgcolor='rgba(0,0,0,0)',
                title="ChopBlock Predictions",
                font_family="Skia",
                showlegend=False
                )

    graphJSON_pred = json.dumps(figPred, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('compareFranchises2.html', graphJSON_pred=graphJSON_pred, graphJSON_adp=graphJSON_adp, graphJSON_shark=graphJSON_shark)

@app.route('/liveScoring')
#@login_required
def liveScoring():
    user_league = session.get("user_league")

    # Get MFL scoring data
    liveScores = get_mfl_liveScoring(user_league)
    # Get Franchises in the league
    franchises = get_mfl_league(user_league)
    # Get all players, sharkRank, and ADP
    predictions = get_df("predictions")

    # merge predictions, franchises, and liveScores
    merged = liveScores.merge(franchises, how='left', on='franchiseID').merge(predictions, how='left', on='id_mfl')
    # Clean
    merged['liveScore'] = merged.liveScore.astype('float64')
    merged['secondsRemaining'] = merged.secondsRemaining.astype('float64')

    # calculate scoreRemaining
    merged['weeklyPred'] = merged['pred'] / 17
    def calcScoreRemaining(row):
        result = ((row['weeklyPred']) * (row['secondsRemaining'] / 3600)) + row['liveScore']
        return result
    merged['scoreRemaining'] = merged.apply(calcScoreRemaining, axis=1)
    # Claculate difference between projection/actual
    merged['diff'] = merged.scoreRemaining - merged.weeklyPred
    # Normalize difference
    merged.loc[merged['diff']>20, 'diff'] = 20
    merged.loc[merged['diff']<-20, 'diff'] = -20
    merged['scaled'] = round(merged['diff'] * 255 / 20, 0)
    merged.dropna(inplace=True)
    merged['scaled'] = merged['scaled'].astype('int')
    # Set colors for chart
    def colorPicker(row):
        scalar = row['scaled']
        if scalar >= 0:
            red = 255 - scalar
            green = 255
            blue = 255 - scalar
        else:
            red = 255 
            green = 255 + scalar
            blue = 255 + scalar
        color = f'rgb({red},{green},{blue})'
        return color
    merged['color'] = merged.apply(colorPicker, axis=1)
    # chart
    players_onthefield = merged.loc[merged.status=="starter"]
    players_onthefield = players_onthefield.sort_values(by='scoreRemaining', ascending=False, ignore_index=True)
    fran_rank = players_onthefield.groupby('franchiseName').sum().sort_values(by='scoreRemaining', ascending=False)
    sorter = fran_rank.index
    players_onthefield.franchiseName = players_onthefield.franchiseName.astype("category")
    players_onthefield.franchiseName.cat.set_categories(sorter, inplace=True)
    players_onthefield.sort_values(["franchiseName"], inplace=True)
    color_discrete_map = dict(zip(players_onthefield.id_mfl, players_onthefield.color))
    # Create bar chart
    figLive = px.bar(players_onthefield, 
                x="franchiseName", 
                y="scoreRemaining", 
                color="player", 
                color_discrete_sequence=list(players_onthefield['color']),
                category_orders={
                    "pos": ["QB", "RB", "WR", "TE", "PK", "DF"]},
                text='player', 
                hover_name="player",
                hover_data={
                    'scoreRemaining':True, 'weeklyPred':True, 'scaled':True,
                    'player':False, 'pos':False, 'franchiseName':False
                    },
                labels={
                    "franchiseName":"Franchise",
                    "predRelative":"Player Value",
                    "pred":"ChopBlock Prediction",
                    "sharkAbsolute":"FantasySharks Prediction",
                    "adpAbsolute":"ADP-Based Prediction"
                }
                )
    figLive.update_layout(
                barmode='stack', 
                xaxis={'categoryorder':'total descending'},
                plot_bgcolor='rgba(0,0,0,0)',
                title="ChopBlock Predictions",
                font_family="Skia",
                showlegend=False
                )
    graphJSON_live = json.dumps(figLive, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('liveScoring.html', graphJSON=graphJSON_live)


@app.route("/logout")
#@login_required
def logout():
    logout_user()
    return render_template("logout.html")