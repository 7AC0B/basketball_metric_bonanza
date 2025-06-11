
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
import numpy as np

DATA_PATH = 'data'
SAVE_PATH = 'scraped_data/gamelogs'

#________________________________________________________________________________________________________________________

base_url = 'https://www.basketball-reference.com'
current_year = 2025
league_years = list(range(2025, current_year+1)) # Earliest is 1950

# Define the directory path
teams_path = Path(f"{DATA_PATH}/scraped_data/teams")  # Replace with your actual path

# The relevant URLs can be found in the teams files
    # Fetch the relevant CSVs, i.e. the roster URL tables for the desired years
csv_files = list(teams_path.glob("*.csv"))
csv_files = [str(p) for p in csv_files]
csv_files = [el for el in csv_files if any(sub in el for sub in [str(l) + '___' for l in league_years])] # only include years specified in league_years
csv_files = [p for p in csv_files if 'roster___urls' in p]

all_player_urls = [pd.read_csv(c).values.flatten().tolist() for c in csv_files]
all_player_urls = set([item for sublist in all_player_urls for item in sublist])
all_player_urls = [url for url in all_player_urls if '/players/' in str(url)]
all_player_urls = [f'{base_url}{player_url}' for player_url in all_player_urls]

YEARS_HINDSIGHT = 1
gamelog_years_to_fetch = list(range(min(league_years) - YEARS_HINDSIGHT, max(league_years) + 1)) # determines how many years in the past we query player gamelogs

gamelog_urls = [[f'{player_url.replace('.html', '')}/gamelog/{year}/' for player_url in all_player_urls] for year in gamelog_years_to_fetch]
gamelog_urls = list(set([item for sublist in gamelog_urls for item in sublist]))

# These urls contain a year -- let's scrape them in order, starting with most recent
def extract_year(url):
    match = int(url[url.find('/gamelog') + 9 : url.find('/gamelog') + 13])
    return match if match else float('inf')  # Use inf to send no-year entries to the end

urls_to_visit = sorted(gamelog_urls, key=extract_year, reverse = True)

#________________________________________________________________________________________________________________________

# Scrape

session = requests.Session()

requested_urls = pd.read_csv(f'{DATA_PATH}/requested_urls.csv')
urls_to_visit = [url for url in gamelog_urls if url not in requested_urls['requested_url'].tolist()]

print('Visiting', len(urls_to_visit), 'URLs.')

status_code_log = []
for url in urls_to_visit:

    print('Progress:', f'{round((urls_to_visit.index(url) / len(urls_to_visit)) * 100, 2)}%')
    print('Est. mins remaining:', int(((len(urls_to_visit) - urls_to_visit.index(url)) * np.mean((3.1, 3.5))) / 60))
    print(url)

    response = session.get(url)

    if response.status_code == 200:

        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract and save data to data_path
        site_id = url[url.find('/players/') + 11 : ].replace('.html', '__') .replace('/', '_')# used in the filename
        extract_and_save_tables(site_soup = soup, site_id = site_id, data_path = DATA_PATH, save_path = SAVE_PATH)

        # Update log of already-requested players
        requested_urls = pd.concat([requested_urls, pd.DataFrame([url], columns = ['requested_url'])], ignore_index = True)
        requested_urls.to_csv(f'{DATA_PATH}/requested_urls.csv', index = False)

        # Update status code log and check if scraper should be stopped
        status_code_log.append(response.status_code)
        any_of_last_5_requests_succesful = 200 in status_code_log[-5:]

        if any_of_last_5_requests_succesful == False:
            print('LAST 5 REQUESTS UNSUCCESFUL - STOPPING SCRAPER.')
            break

    else:
        print('WARNING scrape failed with code', response.status_code, 'for', url)

    time.sleep(random.uniform(3.1, 3.5))  # Basketball Reference has a 20 requests per minute rate limit
    
    print('\n')