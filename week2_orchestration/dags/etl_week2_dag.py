import logging
from datetime import datetime
from typing import Dict
import requests
from airflow.decorators import dag, task
import pandas as pd
import os 
from dotenv import load_dotenv
load_dotenv()

def clean_data(subset: list, dataset: str, DATA_DIR) -> str:
	''' Cleans the specified dataset by removing duplicates and null values.
	Args:
		subset (list): List of columns to check for duplicates.
		dataset (str): Name of the dataset (without .csv extension).
		DATA_DIR (str): Directory where data is stored.
	Returns:
		str: Summary of cleaning operation.
	'''

	# Remove old cleaned file if it exists
	if os.path.exists(f"{DATA_DIR}/cleaned/{dataset}.csv"):
		os.remove(f"{DATA_DIR}/cleaned/{dataset}.csv")

	rowCount = 0
	duplicates = 0
	nulls = 0

	df_iter = pd.read_csv(f"{DATA_DIR}/raw/{dataset}.csv", chunksize=1000)
	for chunk in df_iter:
		# Clean data
		df = chunk 
		df.dropna(inplace=True)
		df.drop_duplicates(subset=subset, inplace=True)
		
		# Track stats
		duplicates += df[subset].duplicated().sum()
		nulls += df.isnull().sum().sum()
		rowCount += len(df)

		# Write to cleaned file
		df.to_csv(f"{DATA_DIR}/cleaned/{dataset}.csv", mode='a', index=False, header=not os.path.exists(f"{DATA_DIR}/cleaned/{dataset}.csv"))

	return f'''{dataset.capitalize()} data cleaned and saved to {DATA_DIR}/cleaned/{dataset}.csv:
		Rows processed: {rowCount}
		Duplicates on {subset}: {duplicates}
		Null values: {nulls}
	'''

@dag(schedule="@daily", start_date=datetime(2021, 12, 1), catchup=False)
def taskflow():
	DATA_DIR = os.getenv("DATA_DIR")
	logging.debug(f"Data directory: {DATA_DIR}")

	@task(task_id="ingest_clickstream")
	def ingest_clickstream() -> None:
		logging.info(clean_data(subset=["session_id"], dataset="clickstream", DATA_DIR=DATA_DIR))
		return

	@task(task_id="ingest_transactions")
	def ingest_transactions() -> None:
		logging.info(clean_data(subset=["txn_id"], dataset="transactions", DATA_DIR=DATA_DIR))
		return

	@task(task_id="ingest_currency_api", retries=2)
	def ingest_currency_api() -> Dict[str, float]:
		API_KEY = os.getenv("API_KEY")
		logging.info(f"API Key: {API_KEY}")

		url = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD" 
		response = requests.get(url) 
		logging.info(f"Currency API response status: {response}")
		data = response.json() 

		if response.status_code == 200 and data["result"] == "success": 
				rates = data["conversion_rates"] 
				logging.info(f"Exchange rates: {rates}")
				return rates
		else: 
			logging.error("API Error:", data)
			return {}

	@task(task_id="transform")
	def transform(exchange_rates) -> None:
		# Remove old transformed files if they exist
		if os.path.exists(f"{DATA_DIR}/transformed/clickstream_utc.csv"):
			os.remove(f"{DATA_DIR}/transformed/clickstream_utc.csv")

		if os.path.exists(f"{DATA_DIR}/transformed/transactions_usd.csv"):
			os.remove(f"{DATA_DIR}/transformed/transactions_usd.csv")


		clickstream_iter = pd.read_csv(f"{DATA_DIR}/cleaned/clickstream.csv", chunksize=1000)
		rowCount_clickstream = 0

		# Transform clickstream data
		for chunk in clickstream_iter:
			clickstream_df = chunk 
			rowCount_clickstream += len(clickstream_df)
			clickstream_df["click_time_utc"] = pd.to_datetime(clickstream_df["click_time"], utc=True)
			clickstream_df.to_csv(f"{DATA_DIR}/transformed/clickstream_utc.csv", mode='a', index=False, header=not os.path.exists(f"{DATA_DIR}/transformed/clickstream_utc.csv"))
		
		transactions_iter = pd.read_csv(f"{DATA_DIR}/cleaned/transactions.csv", chunksize=1000)
		rowCount_transactions = 0

		# Transform transactions data
		for chunk in transactions_iter:
			transactions_df = chunk 
			transactions_df["amount_usd"] = transactions_df.apply(
				lambda row: round(row["amount"] / exchange_rates[row["currency"]], 2), axis=1
			)
			transactions_df["txn_time"] = pd.to_datetime(transactions_df["txn_time"], utc=True)
			rowCount_transactions += len(transactions_df)
			transactions_df.to_csv(f"{DATA_DIR}/transformed/transactions_usd.csv", mode='a', index=False, header=not os.path.exists(f"{DATA_DIR}/transformed/transactions_usd.csv"))

		logging.info(f'''Data transformed and saved to {DATA_DIR}/transformed/(clickstream_utc.csv, transactions_usd.csv):
			Clickstream rows processed: {rowCount_clickstream}
			Transactions rows processed: {rowCount_transactions}
		''')
		return


	[ingest_clickstream(), ingest_transactions()] >> transform(ingest_currency_api())

taskflow()
