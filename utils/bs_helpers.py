import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

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