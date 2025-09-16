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
	@task(task_id="ingest_clickstream")
	def ingest_clickstream() -> pd.DataFrame:
		logging.info(os.listdir())
		df = pd.read_csv("/home/airflow/gcs/data/raw/clickstream.csv")
		logging.info(f"Clickstream rows: {len(df)}")
		return df

	@task(task_id="ingest_transactions")
	def ingest_transactions() -> pd.DataFrame:
		df = pd.read_csv("/home/airflow/gcs/data/raw/transactions.csv")
		logging.info(f"Transactions rows: {len(df)}")
		return df

	clickstream = ingest_clickstream()
	transactions = ingest_transactions()
	
taskflow()
