import os
import configparser
import pandas as pd
from bs4 import BeautifulSoup
import requests


# Load config file
def load_config(config_file):

    config_obj = configparser.ConfigParser()
    config_obj.read(config_file)

    return config_obj


# Create directory if doesn't exists
def create_directory(directory_path):

    if not os.path.exists(directory_path):
        os.makedirs(directory_path)


def parse_tickers(file_source_url):

    # Scrape source URL
    response = requests.get(file_source_url)
    html_content = response.text

    # Parses HTML scraped content
    soup = BeautifulSoup(html_content, 'html.parser')

    # Keep table
    table = soup.find('tbody')

    # Filter for 'td' elements
    td_elements = table.find_all('td', class_ = 'text-center')

    # Retrieve tickers
    tickers = []
    for row in td_elements:

        a_element = row.find('a')

        if a_element:
            ticker = a_element.text
            ticker = f'{ticker}.AX'

            tickers.append(ticker)

    tickers = pd.DataFrame({'tickers': tickers})

    return tickers


def main():

    # Load config file
    config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
    config_obj = load_config(config_file)

    # Retrieve URL of constituents source
    file_source_url = config_obj.get('asx', 'source_tickers')

    # Parse tickers from source URL
    tickers = parse_tickers(file_source_url)
    
    # Create directory to save output file
    output_directory = config_obj.get('directory', 'input')
    create_directory(output_directory)
    
    # Name of output tickers file
    file_tickers = config_obj.get('asx', 'file_tickers')
    tickers.to_csv(os.path.join(output_directory, file_tickers), index=False)

if __name__=='__main__':
    main()