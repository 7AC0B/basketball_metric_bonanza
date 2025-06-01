import pandas as pd
import time
import os
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)
from utils.bs_helpers import get_all_urls_in_html

#________________________________________________________________________________________________________________________

# Get URLs to scrape.
# Basketball-reference lists all links to players in alphabetical order, where each letter has its own URL.
# Derive these URLs:
base_url = "https://www.basketball-reference.com/players/"
urls_to_scrape = [f"{base_url}{letter}/" for letter in "abcdefghijklmnopqrstuvwxyz"]
urls_to_scrape[:2]

#________________________________________________________________________________________________________________________


player_links_found = []

for url in urls_to_scrape:

    print('Getting player links for letter:', url[-2:-1].upper())

    links_found = get_all_urls_in_html(url) # All links

    # Extract player links from all links found
    player_links = [link for link in links_found if link[:45] == 'https://www.basketball-reference.com/players/a/ackerdo01.html'[:45]]
    player_links = [link for link in player_links if link[-5:] == '.html']
    player_links = [link for link in player_links if '/data/' not in link]

    player_links_found.extend(player_links)

    time.sleep(2)

#________________________________________________________________________________________________________________________

# Save
pd.DataFrame(set(player_links_found), columns = ['player_urls']).to_csv('../data/scraped_data/player_urls.csv', index = False)