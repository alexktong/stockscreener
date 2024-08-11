import pandas as pd
import json
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

    with open(file_source, 'r') as file:
        data = json.load(file)

        tickers = []
        for instrument in data['data']['prices']:
            instrument_type = instrument['type']
            
            if instrument_type == 'stocks':
                sgx_ticker = f"{instrument['nc']}.SI"
                tickers.append(sgx_ticker)

    tickers = pd.DataFrame({'tickers': tickers})
    
    return tickers


def main():

    # Load config file
    config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
    config_obj = load_config(config_file)

    # Name of JSON file containing SGX constituents
    raw_tickers = config_obj.get('sgx', 'raw_tickers_json')
    
    # Retrieve URL of constituents source
    file_source_url = config_obj.get('sgx', 'source_tickers', raw=True)

    # Create directory to save output file
    output_directory = config_obj.get('directory', 'input')
    create_directory(output_directory)
    
    # Download SGX constituents into scraped data directory
    scraped_data_directory = config_obj.get('directory', 'scraped_data')
    file_source = os.path.join(scraped_data_directory, raw_tickers)
    os.system(f'wget -O {file_source} {file_source_url}')
   
    # Save ticker column to a CSV file
    file_tickers = config_obj.get('sgx', 'file_tickers')
    
    tickers = parse_tickers(file_source)
    tickers.to_csv(os.path.join(output_directory, file_tickers), index=False)

if __name__=='__main__':
    main()