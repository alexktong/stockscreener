import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import configparser

# Load config file
def load_config(config_file):

    config_obj = configparser.ConfigParser()
    config_obj.read(config_file)

    return config_obj


# Create directory if doesn't exists
def create_directory(directory_path):

    if not os.path.exists(directory_path):
        os.makedirs(directory_path)


def parse_tickers(file_source):

    # Fetch and parse the constituents data
    response = requests.get(file_source)
    soup = BeautifulSoup(response.content, 'xml')

    # Extract text values and find the positions of "Equity"
    values = [row.text for row in soup.find_all('ss:Data')]
    positions = [index - 3 for index, value in enumerate(values) if value == "Equity"]    
    
    # Filter out '--' values and create the DataFrame
    stock_values = [values[pos] for pos in positions if values[pos] != '--']
    tickers = pd.DataFrame({'tickers': stock_values})

    return tickers


def main():

    # Load config file
    config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
    config_obj = load_config(config_file)
    
    # Retrieve URL of constituents source
    file_source = config_obj.get('us', 'source_tickers')

    # Create directory to save output file
    output_directory = config_obj.get('directory', 'input')
    create_directory(output_directory)
    
    # Name of output tickers file
    file_tickers = config_obj.get('us', 'file_tickers')
    
    try:
        # Read Excel file from URL into Pandas dataframe
        tickers = parse_tickers(file_source)

        # Save ticker column to a CSV file    
        tickers = parse_tickers(file_source)
        tickers.to_csv(os.path.join(output_directory, file_tickers), index=False)

    except FileNotFoundError:
        print('Source ticker file not available.')

if __name__=='__main__':
    main()