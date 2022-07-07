# Import dependencies
# Standard python libraries
import json
import os
# Third-party libraries
from flask import Flask, redirect, request, url_for, render_template
from flask_login import (
    UserMixin,
    LoginManager,
    current_user,
    login_required,
    login_user,
    logout_user,
)
from oauthlib.oauth2 import WebApplicationClient
import requests
import pandas as pd
import psycopg2
# Internal imports
from user import User

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
        return (
            "<p>Hello, {}! You are already logged in!</p>"
            "<a class='button' href='/allPlayers'>See All Players</a>"
            "<a class='button' href='/getLeague'>Compare Franchises</a>".format(
                current_user.id
            )
        )
    else:
        return '<a class="button" href="/login">Google Login</a>'

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
        return redirect(url_for("failure"))
    else: 
        # Begin user session by logging the user in
        login_user(user)
        # Send user on to success page
        return redirect(url_for("success"))

@app.route("/logout")
@login_required
def logout():
    logout_user()
    return '<p>You have been logged out! </p><a class="button" href="/">Return to App Homepage</a>' 


@app.route("/allPlayers")
@login_required
def allPlayers():
    con = psycopg2.connect(DATABASE_URL)
    cur = con.cursor()
    # query 
    query = "SELECT * FROM player_df"

    # return results as a dataframe
    results = pd.read_sql(query, con)

    return render_template("allPlayers.html", tables=[results.to_html(classes='data')], titles=results.columns.values)

@app.route('/getLeague')
@login_required
def getLeague():
    return render_template("getLeague.html")

@app.route('/compareFranchises')
@login_required
def compareFranchises():
    league_id = request.args.get("leagueID")

    # Get Franchises in the league
    urlString = f"https://www54.myfantasyleague.com/2022/export?TYPE=league&L={league_id}"
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
    urlString = f"https://www54.myfantasyleague.com/2022/export?TYPE=rosters&L={league_id}"
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
    urlString = f"https://www54.myfantasyleague.com/2022/export?TYPE=freeAgents&L={league_id}"
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
    con = psycopg2.connect(DATABASE_URL)
    cur = con.cursor()
    query = "SELECT * FROM player_df"
    player_df = pd.read_sql(query, con)

    # Merge all dfs
    complete = player_df.merge(rosters_df, on='PlayerID', how='left').merge(franchise_df[['FranchiseID', 'FranchiseName']], on='FranchiseID', how='left')
    complete['FranchiseID'].fillna("FA", inplace=True)
    complete['FranchiseName'].fillna("Free Agent", inplace=True)
    complete['RosterStatus'].fillna("Free Agent", inplace=True)
    complete['SharkRank'].fillna(3000, inplace=True)
    complete['ADP'].fillna(3000, inplace=True)
    complete = complete.sort_values(by=['SharkRank'])
    complete.reset_index(inplace=True, drop=True)

    qbs_2022 = complete[complete['Position'] == "QB"]
    qbs_2022.reset_index(inplace=True, drop=True)
    rbs_2022 = complete[complete['Position'] == "RB"]
    rbs_2022.reset_index(inplace=True, drop=True)
    wrs_2022 = complete[complete['Position'] == "WR"]
    wrs_2022.reset_index(inplace=True, drop=True)
    tes_2022 = complete[complete['Position'] == "TE"]
    tes_2022.reset_index(inplace=True, drop=True)
    pks_2022 = complete[complete['Position'] == "PK"]
    pks_2022.reset_index(inplace=True, drop=True)
    defs_2022 = complete[complete['Position'] == "Def"]
    defs_2022.reset_index(inplace=True, drop=True)

    # Get Relative Values