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

    df = pd.read_excel(file_source, skiprows=2, usecols=['Stock Code', 'Category'], dtype={'Stock Code': str})

    df = df[df['Category'].isin(['Equity', 'Real Estate Investment Trusts'])]

    df.rename(columns={'Stock Code': 'tickers'}, inplace=True)

    tickers = df['tickers'].str[1:] + '.HK'

    return tickers


def main():
    
    # Load config file
    config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
    config_obj = load_config(config_file)

    # Source of HKEX tickers
    file_source = config_obj.get('hkex', 'source_tickers')

    # Create directory to save output file
    output_directory = config_obj.get('directory', 'input')
    create_directory(output_directory)

    # Name of output tickers file
    file_tickers = config_obj.get('hkex', 'file_tickers')

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