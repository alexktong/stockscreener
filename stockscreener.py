import os
import configparser
import json
from tqdm import tqdm
import random
import time
import pandas as pd
import yfinance as yf


# Process stock tickers into a format consistent with Yahoo Finance tickers
def read_tickers(market, file_tickers):
 
	# ASX stock constituents
	if market == 'asx':         
		tickers = pd.read_csv(file_tickers, usecols=['Ticker'])['Ticker']

	# HKEX stock constituents
	elif market == 'hkex':
		tickers = pd.read_csv(file_tickers, usecols=['Stock Code'])['Stock Code']

	# SGX stock constituents
	elif market == 'sgx':
		tickers = pd.read_csv(file_tickers, usecol=['tickers'])['tickers']

	else:
		tickers == None

	return tickers


def calculate_stock_metrics_dict(stock_ticker):

	stock = yf.Ticker(stock_ticker)
	stock_info = stock.info

	income_statement = stock.income_stmt
	income_statement = income_statement.fillna(0)
	
	balance_sheet = stock.balance_sheet
	balance_sheet = balance_sheet.fillna(0)
	
	if len(income_statement) > 0 and len(balance_sheet) > 0:
	
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
			pb = stock_info['priceToBook']
		except KeyError:
			pb = -999

		# Latest P/E
		try:
			pe = stock_info['currentPrice'] / stock_info['trailingEps']
		except (KeyError, ZeroDivisionError):
			pe = -999
		
		# Company information
		try:
			long_name = stock_info['longName']
		except KeyError:
			long_name = 'NA'
		
		# Industry
		try:
			industry = stock_info['industry']
		except KeyError:
			industry = 'N/A'

		# Market cap
		try:
			market_cap = stock_info['marketCap']
			# Convert to millions
			market_cap = market_cap / 1000000
		except KeyError:
			market_cap = -999
						
		# Store stock metrics in a dictionary
		stock_dict = {'ticker': stock_ticker, 'name': long_name, 'industry': industry, 'market_cap (m)': market_cap, 'pb': pb, 'pe': pe, 'roce': roce, 'ebit_margin': ebit_margin, 'interest_coverage': interest_coverage, 'debt_equity': debt_equity}

	else:
		stock_dict = None

	return stock_dict


def random_wait_time_seconds_max(max_seconds):

	random_wait_time = random.uniform(1, max_seconds)
	time.sleep(random_wait_time)


def parse_to_dataframe(list_of_dicts):
	
	df = pd.DataFrame(list_of_dicts)

	df = df.sort_values('roce', ascending=False)

	return df


def main():

	# Load config file
	config_file = 'config.ini'

	config_obj = configparser.ConfigParser()
	config_obj.read(config_file)

	# Loop through stock exchanges
	for market in ['hkex', 'sgx', 'asx']:
		
		# Define output file name
		output_file = config_obj.get('output', f'output_{market}')

		# Retrieve file containing stock tickers
		file_tickers = config_obj.get('input', f'file_tickers_{market}')

		# Post-process stock ticker file
		tickers = read_tickers(market, file_tickers)

		stocks_list = []
		for ticker in tqdm(tickers, desc=f'{market} tickers'):

			# Calculate stock metrics
			stock_dict = calculate_stock_metrics_dict(ticker)

			# Store dictionary in a list if there is content
			if stock_dict:
				stocks_list.append(stock_dict)

			# Wait before proceeding to next loop to reduce risk of IP block
			random_wait_time_seconds_max(4)

		# Create a dataframe of all stock metrics
		stocks_df = parse_to_dataframe(stocks_list)

		# Save dataframe to an output CSV file
		stocks_df.to_csv(output_file, index=False)


if __name__ == '__main__':
	main()
