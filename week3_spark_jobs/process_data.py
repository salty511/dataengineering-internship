import os
from pyspark.sql import SparkSession
import pyspark.pandas as ps
import sys
import logging
from dotenv import load_dotenv
import pandas as pd
import requests
from google.cloud import storage

load_dotenv()

def clean_data(subset: list, df: ps.DataFrame, dataset: str) -> ps.DataFrame:
	''' Cleans the specified dataset by removing duplicates and null values.
	Args:
		subset (list): List of columns to check for duplicates.
		df (ps.DataFrame): Input dataframe to be cleaned.
		dataset (str): Name of the dataset (without .csv extension).
	Returns:
		str: Summary of cleaning operation.
	'''

	# Clean data
	df.dropna(inplace=True)
	df.drop_duplicates(subset=subset, inplace=True)
	
	# Track stats
	duplicates = df[subset].duplicated().sum()
	nulls = df.isnull().sum().sum()
	rowCount = len(df)

	logging.info(f'''{dataset.capitalize()} data cleaned:
		Rows processed: {rowCount}
		Duplicates on {subset}: {duplicates}
		Null values: {nulls}
	''')
	return df

def transform(exchange_rates, transactions_df, clickstream_df) -> tuple[ps.DataFrame, ps.DataFrame]:
	''' Transforms the cleaned datasets by converting datetime strings to UTC and converting transaction amounts to USD.
	Args:
		exchange_rates (Dict[str, float]): Currency exchange rates to USD.
	'''

	rowCount_clickstream = len(clickstream_df)
	rowCount_transactions = len(transactions_df)

	# Timestamp conversion to UTC
	clickstream_df["click_time"] = ps.to_datetime(clickstream_df["click_time"], format="%Y-%m-%dT%H:%M:%SZ")
	transactions_df["txn_time"] = ps.to_datetime(transactions_df["txn_time"], format="%Y-%m-%dT%H:%M:%SZ")

	clickstream_df["partition_date"] = clickstream_df["click_time"].dt.date
	transactions_df["partition_date"] = transactions_df["txn_time"].dt.date

	# Convert transaction amounts to USD
	transactions_df["amount_usd"] = transactions_df.apply(
		lambda row: round(row["amount"] / exchange_rates[row["currency"]], 2), axis=1
	)

	logging.info(f'''Data transformed:
		Clickstream rows processed: {rowCount_clickstream}
		Transactions rows processed: {rowCount_transactions}
	''')
	return clickstream_df, transactions_df

def ingest_currency_api() -> dict[str, float]:
	''' Fetches currency exchange rates from a remote API.
	Returns:
		Dict[str, float]: A dictionary of currency codes and their exchange rates to USD.
	'''

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
	
def read_gcs_file(bucket_name: str, file_path: str) -> pd.DataFrame:
	''' Reads a CSV file from Google Cloud Storage into a pandas-on-Spark DataFrame.
	Args:
		bucket_name (str): Name of the GCS bucket.
		file_path (str): Path to the file within the bucket.
	Returns:
		pd.DataFrame: The loaded DataFrame.
	'''
	gcs_uri = f"gs://{bucket_name}/{file_path}"
	logging.info(f"Reading file from GCS: {gcs_uri}")
	df = pd.read_csv(gcs_uri)
	return df

def buildSparkSession():
	import json
	os.environ['PYSPARK_PYTHON'] = sys.executable
	os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

	with open('sparkconf.json', 'r') as f:
		conf = json.load(f)

	spark = SparkSession.builder\
		.appName("testApp").config(map=conf).getOrCreate()
	
	return spark

def load_parquet_to_gcs(df: pd.DataFrame, dataset: str):
	''' Saves a pandas-on-Spark DataFrame as a Parquet file to Google Cloud Storage.
	Args:
		df (ps.DataFrame): The DataFrame to save.
		dataset (str): Name of the dataset (without .csv extension).
	'''

	output_path = f"gs://dataengineering-internship-test-bucket/data/processed/{dataset}/"
	df.to_parquet(output_path, partition_cols="partition_date", index=False, max_partitions=df["partition_date"].nunique())
	logging.info(f"Saved {dataset} data to {output_path}")

if __name__ == "__main__":
	spark = buildSparkSession()
	
	transactions_df = ps.from_pandas(read_gcs_file("dataengineering-internship-test-bucket", "data/raw/transactions.csv"))
	clickstream_df = ps.from_pandas(read_gcs_file("dataengineering-internship-test-bucket", "data/raw/clickstream.csv"))

	transactions_df = clean_data(subset=["txn_id"], df=transactions_df, dataset="transactions")
	clickstream_df = clean_data(subset=["user_id", "click_time"], df=clickstream_df, dataset="clickstream")

	exchange_rates = ingest_currency_api()
	clickstream_df, transactions_df = transform(exchange_rates, transactions_df, clickstream_df)

	load_parquet_to_gcs(clickstream_df.to_pandas(), "clickstream.parquet")
	load_parquet_to_gcs(transactions_df.to_pandas(), "transactions.parquet")
