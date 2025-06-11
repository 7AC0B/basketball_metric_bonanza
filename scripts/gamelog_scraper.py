import pandas as pd
import time
import os
import sys
import requests
from bs4 import BeautifulSoup
import random
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)
from utils.bs_helpers import extract_and_save_player_data
import re

data_path = 'data'

#________________________________________________________________________________________________________________________

# Derive gamelog URLs from collated player data
base_url = "https://www.basketball-reference.com/"

gamelog_urls_df = pd.read_csv(f'{data_path}/collated_player_data/gamelog_urls.csv')

gamelog_urls_df = gamelog_urls_df.drop_duplicates().reset_index(drop = True)

gamelog_urls = gamelog_urls_df['gamelog_url'].tolist()

urls_to_scrape = [f"{base_url}{gamelog}/" for gamelog in gamelog_urls]

requested_player_urls = pd.read_csv(f'{data_path}/requested_urls.csv')

player_urls_to_visit = [url for url in urls_to_scrape if url not in requested_player_urls['requested_url'].tolist()]

# These urls contain a year -- let's scrape them in order, starting with most recent
def extract_year(url):
    match = re.search(r'/(\d{4})(?!\d)', url)
    return int(match.group(1)) if match else float('inf')  # Use inf to send no-year entries to the end

player_urls_to_visit = sorted(player_urls_to_visit, key=extract_year, reverse = True)

# #________________________________________________________________________________________________________________________

# Scrape

session = requests.Session()

print('Visiting', len(player_urls_to_visit), 'urls.')

status_code_log = []
for url in player_urls_to_visit:

    player_username = url[47:url.find('.html')]

    response = session.get(url)

    print(player_username, response)

    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract and save data to data_path
    extract_and_save_player_data(player_soup = soup, player_username = player_username, data_path = data_path, save_path = 'scraped_data/gamelog_tables', url = url)

    # Update log of already-requested players
    requested_player_urls = pd.concat([requested_player_urls, pd.DataFrame([url], columns = ['requested_url'])], ignore_index = True)
    requested_player_urls.to_csv(f'{data_path}/requested_urls.csv', index = False)

    # Update status code log and check if scraper should be stopped
    status_code_log.append(response.status_code)
    any_of_last_5_requests_succesful = 200 in status_code_log[-5:]

    if any_of_last_5_requests_succesful == False:
        print('LAST 5 REQUESTS UNSUCCESFUL - STOPPING SCRAPER.')
        break

    time.sleep(random.uniform(3.1, 3.5))  # Basketball Reference has a 20 requests per minute rate limit
    
    print('\n')