
import pandas as pd
import time
import os
import sys
import requests
from bs4 import BeautifulSoup
import random
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)
from utils.bs_helpers import extract_and_save_tables
import re

DATA_PATH = 'data'
SAVE_PATH = 'scraped_data/league_overviews'
#________________________________________________________________________________________________________________________

base_url = 'https://www.basketball-reference.com/leagues/NBA_'
current_year = 2025

league_years = list(range(2020, current_year+1)) # Earliest is 1950

urls_to_visit = [f'{base_url}{year}.html' for year in league_years]

# These urls contain a year -- let's scrape them in order, starting with most recent
def extract_year(url):
    match = int(url[49:53])
    return match if match else float('inf')  # Use inf to send no-year entries to the end

urls_to_visit = sorted(urls_to_visit, key=extract_year, reverse = True)
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
        site_id = url[url.find('leagues') + 8 : ].replace('.html', '') # used in the filename
        extract_and_save_tables(site_soup = soup, site_id = site_id, data_path = DATA_PATH, save_path = SAVE_PATH)

    else:
        print('WARNING scrape failed with code', response.status_code, 'for', url)

    time.sleep(random.uniform(3.1, 3.5))  # Basketball Reference has a 20 requests per minute rate limit
    
    print('\n')