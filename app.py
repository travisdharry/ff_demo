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
from bs4 import BeautifulSoup
from oauthlib.oauth2 import WebApplicationClient
import requests
import pandas as pd
import psycopg2
import plotly
import plotly.express as px
import plotly.graph_objects as go

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
    # Query database for player_df
    con = psycopg2.connect(DATABASE_URL)
    cur = con.cursor()
    player_query = "SELECT * FROM player_df"
    player_df = pd.read_sql(player_query, con)

    return render_template("allPlayers.html", tables=[player_df.to_html(classes='data')], titles=player_df.columns.values)

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

    # Split complete df by player position
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
    qbs_2022 = pd.merge(qbs_2022, qb_proj, how="left", left_index=True, right_index=True)
    rbs_2022 = pd.merge(rbs_2022, rb_proj, how="left", left_index=True, right_index=True)
    wrs_2022 = pd.merge(wrs_2022, wr_proj, how="left", left_index=True, right_index=True)
    tes_2022 = pd.merge(tes_2022, te_proj, how="left", left_index=True, right_index=True)
    pks_2022 = pd.merge(pks_2022, pk_proj, how="left", left_index=True, right_index=True)
    defs_2022 = pd.merge(defs_2022, def_proj, how="left", left_index=True, right_index=True)

    qbs_top = qbs_2022.sort_values(by='Projection_Relative', ascending=False, ignore_index=True).groupby('FranchiseName').head(1)
    rbs_top = rbs_2022.sort_values(by='Projection_Relative', ascending=False, ignore_index=True).groupby('FranchiseName').head(2)
    wrs_top = wrs_2022.sort_values(by='Projection_Relative', ascending=False, ignore_index=True).groupby('FranchiseName').head(3)
    tes_top = tes_2022.sort_values(by='Projection_Relative', ascending=False, ignore_index=True).groupby('FranchiseName').head(2)

    qbs_remainder = qbs_2022[~qbs_2022['PlayerID'].isin(qbs_top['PlayerID'])].groupby('FranchiseName').head(1)
    rbs_remainder = rbs_2022[~rbs_2022['PlayerID'].isin(rbs_top['PlayerID'])].groupby('FranchiseName').head(3)
    wrs_remainder = wrs_2022[~wrs_2022['PlayerID'].isin(wrs_top['PlayerID'])].groupby('FranchiseName').head(3)
    tes_remainder = tes_2022[~tes_2022['PlayerID'].isin(tes_top['PlayerID'])].groupby('FranchiseName').head(3)

    remainder = pd.concat([qbs_remainder, rbs_remainder, wrs_remainder, tes_remainder])

    top_remainders = remainder.sort_values(by='Projection_Absolute', ascending=False, ignore_index=True).groupby('FranchiseName').head(3)

    fran_rost = pd.concat([qbs_top, rbs_top, wrs_top, tes_top, top_remainders])
    fran_rost = fran_rost.sort_values(by='Projection_Absolute', ascending=False, ignore_index=True)

    fran_rank = fran_rost.groupby('FranchiseName').sum().sort_values(by='Projection_Absolute', ascending=False)

    sorter = fran_rank.index

    fran_rost.FranchiseName = fran_rost.FranchiseName.astype("category")
    fran_rost.FranchiseName.cat.set_categories(sorter, inplace=True)
    fran_rost.sort_values(["FranchiseName"], inplace=True)

    fig = px.bar(fran_rost, 
                x="FranchiseName", 
                y="Projection_Relative", 
                color="Position_x", 
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
#    return render_template("allPlayers.html", tables=[point_projections.to_html(classes='data')])
