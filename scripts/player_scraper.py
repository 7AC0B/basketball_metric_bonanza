import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random

# Functions

def get_overheader_multi_index(table):
    # Get the header rows
    rows = table.find_all("tr")
    level_0 = []
    level_1 = []

    # First header row - groups like "Per Game", "Totals", etc.
    header_row_1 = rows[0].find_all(["th", "td"])
    for cell in header_row_1:
        colspan = int(cell.get("colspan", 1))
        label = cell.text.strip() or ""
        for _ in range(colspan):
            level_0.append(label)

    # Second header row - actual column names
    header_row_2 = rows[1].find_all(["th", "td"])
    level_1 = [cell.text.strip() for cell in header_row_2]

    # Truncate or pad level_0 to match length of level_1
    if len(level_0) > len(level_1):
        level_0 = level_0[:len(level_1)]
    elif len(level_0) < len(level_1):
        level_0.extend([""] * (len(level_1) - len(level_0)))

    # Zip into tuples for pandas MultiIndex
    multi_index = list(zip(level_0, level_1))

    return multi_index

def save_player_table(player_soup, table_id = 'totals_stats'):
    table = player_soup.find('table', id = table_id)

    table_rows = table.find('tbody').find_all(['tr'])
    table_data = [[row_value.text for row_value in row_elements.find_all(['th', 'td'])] for row_elements in table_rows]
    table_has_overheader = len(table.find_all('tr', class_='over_header')) > 0

    if table_has_overheader:

        multi_index_headers = get_overheader_multi_index(table)
        table_df = pd.DataFrame(table_data, columns=pd.MultiIndex.from_tuples(multi_index_headers))

    else:

        colheaders = [colheader.text for colheader in table.find('thead').find_all('th')]
        table_df = pd.DataFrame(table_data, columns = colheaders)

    return table_df

def get_player_gamelog_urls(player_soup):
    all_gamelog_link_elements = list(set([link['href'] for link in player_soup.find_all('a') if '/gamelog/' in str(link)]))
    all_gamelog_link_elements = list(set([link[:-1] if link.endswith('/') else link for link in all_gamelog_link_elements]))
    return pd.DataFrame(all_gamelog_link_elements, columns=['gamelog_url'])

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

player_urls_to_visit = [url for url in all_player_urls if url not in requested_player_urls['requested_url'].tolist()]
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