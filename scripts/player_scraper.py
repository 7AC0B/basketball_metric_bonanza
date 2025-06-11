import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random
import os
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)
from utils.bs_helpers import save_player_table, get_player_gamelog_urls

# Functions

def extract_and_save_player_data(player_soup, player_username, data_path = '../data'):

    table_ids_found_on_page = [table_element['id'] for table_element in player_soup.find_all('table')]

    print('Found', len(table_ids_found_on_page), 'table ids.')

    ### FIND AND SAVE ALL DATATABLES IN PLAYER SOUP ###
    for table_id in table_ids_found_on_page:

        try:
            table_found = save_player_table(player_soup = player_soup, table_id = table_id)
            table_found.to_csv(f'{data_path}/scraped_data/player_tables/{player_username}___{table_id}.csv')

        except:
            print((f'*** FAILED TO FETCH {table_id} ***'))

            # Save soup along with table_id for future retry
            with open(f'{data_path}/failed_table_scrapes/{player_username}___table_id___{table_id}.html', "w", encoding="utf-8") as f:
                f.write(str(player_soup))
            
    ### FIND AND SAVE ALL GAMELOG URLS FOUND IN PLAYER SOUP ###
    try:
        table_found = get_player_gamelog_urls(player_soup = player_soup)
        table_found.to_csv(f'{data_path}/scraped_data/player_tables/{player_username}___gamelog_urls.csv')
    except:
        print((f'*** FAILED TO FETCH gamelog_urls ***'))

        # Save soup along with table_id for future retry
        with open(f'{data_path}/failed_table_scrapes/{player_username}___gamelog_urls.html', "w", encoding="utf-8") as f:
            f.write(str(player_soup))

# Scrape

data_path = 'data'

session = requests.Session()

all_player_urls = pd.read_csv(f'{data_path}/scraped_data/player_urls.csv')['player_urls'].tolist()
requested_player_urls = pd.read_csv(f'{data_path}/requested_urls.csv')

player_urls_to_visit = all_player_urls[:1]# [url for url in all_player_urls if url not in requested_player_urls['requested_url'].tolist()]
status_code_log = []
for url in player_urls_to_visit:

    player_username = url[47:url.find('.html')]

    response = session.get(url)

    print(player_username, response)

    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract and save data to data_path
    extract_and_save_player_data(player_soup = soup, player_username = player_username, data_path = data_path)

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