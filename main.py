import configparser
import json
from tqdm import tqdm
import random
import time
import pandas as pd
import yfinance as yf

# Load config file
config_file = 'config.ini'

config_obj = configparser.ConfigParser()
config_obj.read(config_file)


# Process stock tickers into a format consistent with Yahoo Finance tickers
def read_tickers(file_tickers):
 
    tickers = ''
     
    # ASX stock constituents
    if market == 'asx':         
        tickers = pd.read_csv(file_tickers, usecols=['Ticker'])['Ticker']


    # HKEX stock constituents
    elif market == 'hkex':
        tickers = pd.read_csv(file_tickers, usecols=['Stock Code'])['Stock Code']

    # SGX stock constituents
    elif market == 'sgx':
        with open(file_tickers, 'r') as file:
            data = json.load(file)

        tickers = []
        for stock in data['data']['prices']:
            sgx_ticker = f"{stock['nc']}.SI"
            tickers.append(sgx_ticker)

    return tickers


def calculate_stock_metrics_dict(stock_ticker, income_statement, balance_sheet):

	# Average ROCE
	try:
	# ROCE = EBIT / Total Assets
		roce = (income_statement.loc['Pretax Income'] + income_statement.loc['Interest Expense']) / balance_sheet.loc['Total Assets']
		roce = roce.mean()
	except (KeyError, ZeroDivisionError):
		roce = -999

	# Average EBIT margin
	try:
		ebit_margin = (income_statement.loc['Pretax Income'] + income_statement.loc['Interest Expense']) / income_statement.loc['Total Revenue']
		ebit_margin = ebit_margin.mean()
	except (KeyError, ZeroDivisionError):
		ebit_margin = -999

	# Average interest coverage
	try:
		interest_coverage = (income_statement.loc['Pretax Income'] + income_statement.loc['Interest Expense']) / income_statement.loc['Interest Expense']
		interest_coverage = interest_coverage.mean()
	except (KeyError, ZeroDivisionError):
		interest_coverage = -999

	# Latest debt / equity ratio       
	try:
		debt_equity = balance_sheet.loc['Total Debt'] / balance_sheet.loc['Stockholders Equity']
		debt_equity = debt_equity.iloc[0]
	except (KeyError, ZeroDivisionError):
		debt_equity = -999

	# Latest P/B
	try:
		pb = stock.info['priceToBook']
	except KeyError:
		pb = -999

	# Latest P/E
	try:
		pe = stock.info['currentPrice'] / stock.info['trailingEps']
	except (KeyError, ZeroDivisionError):
		pe = -999
	
	# Company information
	try:
		long_name = stock.info['longName']
	except KeyError:
		long_name = 'NA'
	
	# Industry
	try:
		industry = stock.info['industry']
	except KeyError:
		industry = 'N/A'

	# Market cap
	try:
		market_cap = stock.info['marketCap']
		# Convert to millions
		market_cap = market_cap / 1000000
	except KeyError:
		market_cap = -999
					
	# Store stock metrics in a dictionary
	stock_dict = {'ticker': stock_ticker, 'name': long_name, 'industry': industry, 'market_cap (m)': market_cap, 'pb': pb, 'pe': pe, 'roce': roce, 'ebit_margin': ebit_margin, 'interest_coverage': interest_coverage, 'debt_equity': debt_equity}

	return stock_dict


for market in ['hkex', 'sgx', 'asx']:
	
	# Define output file name
	output_file = config_obj.get('output', f'output_{market}')

	# Retrieve file containing stock tickers
	file_tickers = config_obj.get('input', f'tickers_{market}')

	# Post-process stock ticker file
	tickers = read_tickers(file_tickers)

	stocks_list = []

	for ticker in tqdm(tickers, desc=f'{market} tickers'):

		# Retrieve stock information
		stock = yf.Ticker(ticker)

		statement_income = stock.income_stmt
		statement_income = statement_income.fillna(0)

		statement_balance = stock.balance_sheet
		statement_balance = statement_balance.fillna(0)

		# Calculate metrics if financial information is available
		if len(statement_income) > 0 and len(statement_balance) > 0:

			# Calculate stock metrics
			stock_dict = calculate_stock_metrics_dict(ticker, statement_income, statement_balance)

			# Store dictionary in a list
			stocks_list.append(stock_dict)

			# Wait before proceeding to next loop to reduce risk of IP block
			random_wait_time = random.randint(1, 4)
			time.sleep(random_wait_time)

	# Create a dataframe of all stock metrics
	stocks_df = pd.DataFrame(stocks_list)

	# Sort by ROCE
	stocks_df = stocks_df.sort_values('roce', ascending=False)

	# Save dataframe to an output CSV file
	stocks_df.to_csv(output_file, index=False)