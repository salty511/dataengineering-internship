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

	[ingest_clickstream(), ingest_transactions()] >> ingest_currency_api()

taskflow()
