import datetime
import os
import configparser
from tqdm import tqdm
import random
import time
import pandas as pd
import yfinance as yf


# Load config file
def load_config(config_file):

    config_obj = configparser.ConfigParser()
    config_obj.read(config_file)

    return config_obj


def create_directory(directory_path):
	
	if not os.path.exists(directory_path):
		os.makedirs(directory_path)
	else:
		for file_name in os.listdir(directory_path):
			file_path = os.path.join(directory_path, file_name)

			if os.path.isfile(file_path):
				os.unlink(file_path)


# Process stock tickers into a format consistent with Yahoo Finance tickers
def read_tickers(market, file_tickers):
 
	# ASX stock constituents
	if market == 'asx':         
		tickers = pd.read_csv(file_tickers, usecols=['tickers'])['tickers']

	# HKEX stock constituents
	elif market == 'hkex':
		tickers = pd.read_csv(file_tickers, usecols=['Stock Code'])['Stock Code']

	# SGX stock constituents
	elif market == 'sgx':
		tickers = pd.read_csv(file_tickers, usecols=['tickers'])['tickers']

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
	
		# ROCE
		try:
			# ROCE = EBIT / Total Assets
			roce = (income_statement.loc['Pretax Income'] + income_statement.loc['Interest Expense']) / balance_sheet.loc['Total Assets']
			roce_cy = roce.iloc[0] # cy ROCE
			roce_avg = roce.mean() # Average ROCE
		except (KeyError, ZeroDivisionError):
			roce_cy = -999
			roce_avg = -999

		# Average EBIT margin
		try:
			ebit_margin = (income_statement.loc['Pretax Income'] + income_statement.loc['Interest Expense']) / income_statement.loc['Total Revenue']
			ebit_margin = ebit_margin.mean()
		except (KeyError, ZeroDivisionError):
			ebit_margin = -999

		# Average interest coverage
		try:
			interest_coverage = (income_statement.loc['Pretax Income'] + income_statement.loc['Interest Expense']) / income_statement.loc['Interest Expense']
			interest_coverage_cy = interest_coverage.iloc[0] # cy interest coverage
			interest_coverage_avg = interest_coverage.mean() # Average interest coverage
		except (KeyError, ZeroDivisionError):
			interest_coverage_cy = -999
			interest_coverage_avg = -999

		# cy debt / equity ratio       
		try:
			debt_equity = balance_sheet.loc['Total Debt'] / balance_sheet.loc['Stockholders Equity']
			debt_equity_cy = debt_equity.iloc[0] # cy debt-equity ratio
			debt_equity_avg = debt_equity.mean() # Average debt-equity ratio
		except (KeyError, ZeroDivisionError):
			debt_equity_cy = -999
			debt_equity_avg = -999

		# cy P/B
		try:
			pb = stock_info['priceToBook']
		except KeyError:
			pb = -999

		# cy P/E
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

		# cy cash / equity ratio
		try:
			cash_assets = balance_sheet.loc['Cash Cash Equivalents And Short Term Investments'] / balance_sheet.loc['Total Assets']
			cash_assets = cash_assets.iloc[0] # cy ratio
		except (KeyError, ZeroDivisionError):
			cash_assets = -999			
					
		# Store stock metrics in a dictionary
		stock_dict = {'ticker': stock_ticker, 'name': long_name, 'industry': industry, 'mktcap (m)': market_cap, 'pb': pb, 'pe': pe, 'roce_cy': roce_cy, 'roce_avg': roce_avg, 'ebit_margin': ebit_margin, 'interest_cov_cy': interest_coverage_cy, 'interest_cov_avg': interest_coverage_avg, 'debt_equity_cy': debt_equity_cy, 'debt_equity_avg': debt_equity_avg, 'cash_assets': cash_assets}

	else:
		stock_dict = None

	return stock_dict


def random_wait_time_seconds_max(max_seconds):

	random_wait_time = random.uniform(1, max_seconds)
	time.sleep(random_wait_time)


def parse_to_dataframe(list_of_dicts):
	
	df = pd.DataFrame(list_of_dicts)

	df = df.sort_values('roce_cy', ascending=False)

	return df


def screener_real_estate_low_pb(df, max_pb_ratio=0.7):

	# Real estate only
	list_of_real_estate_industries = ['reit', 'real estate', 'lodging']
	df_segment = df[df['industry'].str.contains('|'.join(list_of_real_estate_industries), case=False)]

	# Low PB
	df_segment = df_segment[df_segment['pb'].between(0, max_pb_ratio)]

	return df_segment


def screener_net_net(df, max_pb_ratio=0.8, min_cash_assets_ratio=0.5):

	# Low PB
	df_segment = df[df['pb'].between(0, max_pb_ratio)]
	
	# High cash ratio
	df_segment = df_segment[df_segment['cash_assets'] >= min_cash_assets_ratio]

	return df_segment


def screener_low_debt(df, max_debt_ratio=0.15):

	# Low debt
	df_segment = df[df['debt_equity_cy'] <= max_debt_ratio]

	return df_segment


def main():

	# Load config file
	config_file = os.path.join(os.path.dirname(__file__), 'config.ini')
	config_obj = load_config(config_file)

	input_directory = config_obj.get('default', 'input_directory')
	output_directory = config_obj.get('default', 'output_directory')
	create_directory(output_directory)

	# Loop through stock exchanges
	markets = config_obj.get('default', 'markets').split(', ')
	for market in markets:

		# Retrieve file containing stock tickers
		file_tickers = config_obj.get(market, 'file_tickers')

		# Post-process stock ticker file
		tickers = read_tickers(market, os.path.join(input_directory, file_tickers))

		stocks_list = []
		for ticker in tqdm(tickers, desc=f'{market} tickers'):

			# Calculate stock metrics
			stock_dict = calculate_stock_metrics_dict(ticker)

			# Store dictionary in a list if there is content
			if stock_dict:
				stocks_list.append(stock_dict)

			# Wait before proceeding to next loop to reduce risk of IP block
			random_wait_time_seconds_max(3)

		# Create a dataframe of all stock metrics
		stocks_df = parse_to_dataframe(stocks_list)

		# Define output file name
		output_file = config_obj.get(market, 'file_all')
		stocks_df.to_csv(os.path.join(output_directory, output_file), index=False)

		# Select screeners
		output_filtered_1 = config_obj.get(market, 'file_real_estate_low_pb')
		stocks_real_estate_low_pb_df = screener_real_estate_low_pb(stocks_df)
		stocks_real_estate_low_pb_df.to_csv(os.path.join(output_directory, output_filtered_1), index=False)

		output_filtered_2 = config_obj.get(market, 'file_net_net')
		stocks_net_net_df = screener_net_net(stocks_df)
		stocks_net_net_df.to_csv(os.path.join(output_directory, output_filtered_2), index=False)

		output_filtered_3 = config_obj.get(market, 'file_low_debt')
		stocks_low_Debt_df = screener_low_debt(stocks_df)
		stocks_low_Debt_df.to_csv(os.path.join(output_directory, output_filtered_3), index=False)

if __name__ == '__main__':
	main()
