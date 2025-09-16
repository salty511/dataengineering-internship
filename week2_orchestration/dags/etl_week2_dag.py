import logging
from datetime import datetime
from typing import Dict
import requests
from airflow.decorators import dag, task
import pandas as pd
import os 
from dotenv import load_dotenv
load_dotenv()

@dag(schedule="@daily", start_date=datetime(2021, 12, 1), catchup=False)
def taskflow():
	DATA_DIR = os.getenv("DATA_DIR")
	logging.debug(f"Data directory: {DATA_DIR}")

	@task(task_id="ingest_clickstream")
	def ingest_clickstream() -> pd.DataFrame:
		df_iter = pd.read_csv(f"{DATA_DIR}/raw/clickstream.csv", chunksize=1000)
		rowCount = 0
		duplicates = 0
		nulls = 0

		for chunk in df_iter:
			# Clean data
			df = chunk 
			df.dropna(inplace=True)
			df.drop_duplicates(subset=["session_id"], inplace=True)
			
			# Track stats
			duplicates += df["session_id"].duplicated().sum()
			nulls += df.isnull().sum().sum()
			rowCount += len(df)

			# Write to cleaned file
			df.to_csv(f"{DATA_DIR}/cleaned/clickstream.csv", mode='a', index=False, header=not os.path.exists(f"{DATA_DIR}/cleaned/clickstream.csv"))

		logging.info(f'''Clickstream data cleaned and saved to {DATA_DIR}/cleaned/clickstream.csv:
			Rows processed: {rowCount}
			Duplicates on session_id: {duplicates}
			Null values: {nulls}
		''')
		return df

	@task(task_id="ingest_transactions")
	def ingest_transactions() -> pd.DataFrame:
		df = pd.read_csv(f"{DATA_DIR}/raw/transactions.csv")
		logging.info(f"Transactions rows: {len(df)}")
		return df

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

	clickstream = ingest_clickstream()
	transactions = ingest_transactions()
	exchange_rates = ingest_currency_api()

taskflow()
