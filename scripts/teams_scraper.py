
import pandas as pd
import time
import os
import sys
import requests
from bs4 import BeautifulSoup
import random
from pathlib import Path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)
from utils.bs_helpers import extract_and_save_tables

import re

DATA_PATH = 'data'
SAVE_PATH = 'scraped_data/teams'

#________________________________________________________________________________________________________________________

base_url = 'https://www.basketball-reference.com'
current_year = 2025
league_years = list(range(2025, current_year+1)) # Earliest is 1950

# Define the directory path
league_overviews_path = Path(f"{DATA_PATH}/scraped_data/league_overviews")  # Replace with your actual path

# The relevant URLs can be found in the league overviews
    # Fetch the relevant CSVs, i.e. the conference standings URL tables for the desired years
csv_files = list(league_overviews_path.glob("*.csv"))
csv_files = [str(p) for p in csv_files]
csv_files = [el for el in csv_files if any(sub in el for sub in ['NBA_' + str(l) for l in league_years])] # only include years specified in league_years
csv_files = [p for p in csv_files if 'confs_standings_' in p]
csv_files = [p for p in csv_files if '___urls' in p]

all_team_league_urls = [pd.read_csv(c).values.flatten().tolist() for c in csv_files]
all_team_league_urls = set([item for sublist in all_team_league_urls for item in sublist])
all_team_league_urls = [url for url in all_team_league_urls if '/teams/' in str(url)]
all_team_league_urls = [f'{base_url}/{team_url}' for team_url in all_team_league_urls]

# These urls contain a year -- let's scrape them in order, starting with most recent
def extract_year(url):
    match =int(url[url.find('.html') - 4 : url.find('.html')])
    return match if match else float('inf')  # Use inf to send no-year entries to the end

urls_to_visit = sorted(all_team_league_urls, key=extract_year, reverse = True)
#________________________________________________________________________________________________________________________

# Scrape

session = requests.Session()

status_code_log = []
for url in urls_to_visit:
    print(url)

    response = session.get(url)

    if response.status_code == 200:

        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract and save data to data_path
        site_id = url[url.find('/teams/') + 7 : ].replace('.html', '') .replace('/', '_')# used in the filename
        extract_and_save_tables(site_soup = soup, site_id = site_id, data_path = DATA_PATH, save_path = SAVE_PATH)

    else:
        print('WARNING scrape failed with code', response.status_code, 'for', url)

    time.sleep(random.uniform(3.1, 3.5))  # Basketball Reference has a 20 requests per minute rate limit
    
    print('\n')