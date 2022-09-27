# import data analysis tools
import pandas as pd
import os

# import scraping tools
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

# scrape FF DB
def scrape_ffdb(seasonStart, seasonEnd, weekStart, weekEnd):
    # Initialize overall df
    #####
    # loop through positions
    posList = ['QB', 'RB', 'WR', 'TE', 'K', 'DST']
    for position in posList:
        # Set column names
        colsOffense = [
            'Unnamed: 0_level_0_Player', 'Unnamed: 1_level_0_Game', 'Unnamed: 2_level_0_Pts*', 
            'Passing_Att', 'Passing_Cmp', 'Passing_Yds', 'Passing_TD', 'Passing_Int', 'Passing_2Pt', 
            'Rushing_Att', 'Rushing_Yds', 'Rushing_TD', 'Rushing_2Pt', 
            'Receiving_Rec', 'Receiving_Yds', 'Receiving_TD', 'Receiving_2Pt', 
            'Fumbles_FL', 'Fumbles_TD'] 
        colsK = ['Player', 'Game', 'Pts*', 'XPA', 'XPM', 'FGA', 'FGM', '50+'] 
        colsDST = ['Team', 'Game', 'Pts*', 'Sack', 'Int', 'Saf', 'FR', 'Blk', 'TD', 'PA', 'PassYds', 'RushYds', 'TotYds']
    
        colMap = {"QB":colsOffense, "RB":colsOffense, "WR":colsOffense, "TE":colsOffense, "K":colsK, "DST":colsDST}
        colNamesInit = colMap.get(position)
        # Initialize empty df
        df=pd.DataFrame(columns=colNamesInit)

        # Set Selenium/Chrome settings
        chrome_options = webdriver.ChromeOptions()
        chrome_options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--no-sandbox")
        capa = DesiredCapabilities.CHROME
        capa["pageLoadStrategy"] = "none"

        # Loop through seasons
        for i in range(seasonStart, seasonEnd):
            # Loop through weeks
            for j in range(weekStart, weekEnd):
                season = i
                week = j

                # Scrape web for stats
                url = f"https://www.footballdb.com/fantasy-football/index.html?pos={position}&yr={season}&wk={week}&key=48ca46aa7d721af4d58dccc0c249a1c4"

                driver = webdriver.Chrome(
                            executable_path=os.environ.get("CHROMEDRIVER_PATH"), 
                            chrome_options=chrome_options, 
                            desired_capabilities=capa)
                wait = WebDriverWait(driver, 20)
                driver.get(url)

                wait.until(EC.presence_of_element_located((By.XPATH, "//div[@id='leftcol']/div[3]/table")))
                driver.execute_script("window.stop();")

                result = pd.read_html(driver.find_element(By.XPATH, value="//div[@id='leftcol']/div[3]/table").get_attribute("outerHTML"))
                regex_result = driver.find_element(By.XPATH, value="//div[@id='leftcol']/div[3]/table/tbody").get_attribute("outerHTML")

                # find player's team based on which team is bolded
                regex_teams = re.findall("<b>(.*?)</b>", regex_result)

                # flatten multiindex
                result = result[0]
                result.columns = result.columns.get_level_values(0) + '_' +  result.columns.get_level_values(1)

                # Set values for new columns
                result['team'] = regex_teams
                result['season'] = season
                result['week'] = week

                # concatenate to master df
                df = pd.concat([df, result], axis=0, ignore_index=True)


