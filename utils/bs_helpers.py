import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd


def get_all_urls_in_html(url):

    # Fetch and parse page
    response = requests.get(url)
    print(response)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all URLs
    links = soup.find_all('a')
    urls = [link.get('href') for link in links if link.get('href')]

    # Convert relative URLs to absolute ones
    absolute_urls = [urljoin(url, u) for u in urls]

    return absolute_urls


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

def get_table_df(site_soup, table_id = 'totals_stats'):
    table = site_soup.find('table', id = table_id)

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

def get_table_df_dict(site_soup, table_id = 'totals_stats'):
    table = site_soup.find('table', id = table_id)

    table_rows = table.find('tbody').find_all(['tr'])
    table_data = [[row_value.text for row_value in row_elements.find_all(['th', 'td'])] for row_elements in table_rows]
    table_urls = [[row_value.find('a')['href'] if row_value.find('a') != None else None for row_value in row_elements.find_all(['th', 'td'])] for row_elements in table_rows]

    table_has_overheader = len(table.find_all('tr', class_='over_header')) > 0

    if table_has_overheader:

        multi_index_headers = get_overheader_multi_index(table)
        data_table_df = pd.DataFrame(table_data, columns=pd.MultiIndex.from_tuples(multi_index_headers))
        url_table_df = pd.DataFrame(table_urls, columns=pd.MultiIndex.from_tuples(multi_index_headers))

    else:

        colheaders = [colheader.text for colheader in table.find('thead').find_all('th')]
        data_table_df = pd.DataFrame(table_data, columns = colheaders)
        url_table_df = pd.DataFrame(table_urls, columns = colheaders)

    return {'data_table' : data_table_df, 'url_table' : url_table_df}

def get_player_gamelog_urls(player_soup):
    all_gamelog_link_elements = list(set([link['href'] for link in player_soup.find_all('a') if '/gamelog/' in str(link)]))
    all_gamelog_link_elements = list(set([link[:-1] if link.endswith('/') else link for link in all_gamelog_link_elements]))
    return pd.DataFrame(all_gamelog_link_elements, columns=['gamelog_url'])

def extract_and_save_tables(site_soup, site_id, data_path = '../data', save_path = 'scraped_data/player_tables'):

    table_ids_found_on_page = [table_element['id'] for table_element in site_soup.find_all('table') if table_element.has_attr('id')]

    print('Found', len(table_ids_found_on_page), 'table ids.')

    ### FIND AND SAVE ALL DATATABLES IN PLAYER SOUP ###
    for table_id in table_ids_found_on_page:

        # try:
        tables_found = get_table_df_dict(site_soup = site_soup, table_id = table_id)

        tables_found['data_table'].to_csv(f'{data_path}/{save_path}/{site_id}___{table_id}.csv')
        tables_found['url_table'].to_csv(f'{data_path}/{save_path}/{site_id}___{table_id}___urls.csv')

        # except:
        #     print((f'*** FAILED TO FETCH {table_id} ***'))

        #     # Save soup along with table_id for future retry
        #     with open(f'{data_path}/failed_table_scrapes/{site_id}___{table_id}.html', "w", encoding="utf-8") as f:
        #         f.write(str(site_soup))
            