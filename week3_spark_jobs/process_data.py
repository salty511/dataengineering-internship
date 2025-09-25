import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as sdf
import pyspark.sql.types as T
from pyspark.sql.functions import udf
import pandas as pd
import sys
import logging
from dotenv import load_dotenv
import pandas as pd
import requests

load_dotenv()

def clean_data(subset: list, sdf: sdf, dataset: str) -> sdf:
	''' Cleans the specified dataset by removing duplicates and null values.
	Args:
		subset (list): List of columns to check for duplicates.
		df (ps.DataFrame): Input dataframe to be cleaned.
		dataset (str): Name of the dataset (without .csv extension).
	Returns:
		str: Summary of cleaning operation.
	'''
	sdf_out = sdf
	rowCount = sdf.count()

	# Clean data
	sdf_out = sdf.dropna()
	nulls = rowCount - sdf_out.count()

	sdf_out = sdf_out.dropDuplicates(subset=subset)
	duplicates = rowCount - nulls - sdf_out.count()

	logging.info(f'''{dataset.capitalize()} data cleaned:
		Rows processed: {rowCount}
		Duplicates on {subset}: {duplicates}
		Null values: {nulls}
	''')
	return sdf


def transform(spark, exchange_rates, transactions_df, clickstream_df) -> tuple[sdf, sdf]:
	''' Transforms the cleaned datasets by converting datetime strings to UTC and converting transaction amounts to USD.
	Args:
		spark: SparkSession instance.
		exchange_rates (Dict[str, float]): Currency exchange rates to USD.
	'''

	rowCount_clickstream = clickstream_df.count()
	rowCount_transactions = transactions_df.count()

	broadcast_rates = spark.sparkContext.broadcast(exchange_rates)

	def convert_to_usd(currency, amount):
		rate = broadcast_rates.value.get(currency, 1.0)
		return amount / rate if rate != 0 else 0.0

	convert_udf = udf(convert_to_usd, T.FloatType())

	clickstream_df = clickstream_df.withColumn("partition_date", clickstream_df["click_time"].cast("date"))
	transactions_df = transactions_df.withColumn("partition_date", transactions_df["txn_time"].cast("date"))

	transactions_df = transactions_df.withColumn("amount_usd", convert_udf("currency", "amount"))

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


def read_gcs_file(bucket_name: str, file_path: str, spark, schema: str, gcs: bool) -> sdf:
	''' Reads a CSV file from Google Cloud Storage into a pandas-on-Spark DataFrame.
	Args:
		bucket_name (str): Name of the GCS bucket.
		file_path (str): Path to the file within the bucket.
	Returns:
		pd.DataFrame: The loaded DataFrame.
	'''
	if gcs:
		gcs_uri = f"gs://{bucket_name}/{file_path}"

		logging.info(f"Reading file from GCS: {gcs_uri}")

		sdf = spark.read.csv(gcs_uri, header=True, schema=schema)

		logging.info(f"File {file_path} opened successfully.")
	else:
		sdf = spark.read.csv(file_path, header=True, schema=schema)
	return sdf

def buildSparkSession(gcs):
	import json
	os.environ['PYSPARK_PYTHON'] = sys.executable
	os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

	if gcs:
		spark = SparkSession.builder\
		.appName("testApp").getOrCreate()
		
	else:
		with open('sparkconf.json', 'r') as f:
			conf = json.load(f)
		spark = SparkSession.builder\
		.appName("testApp").config(map=conf).getOrCreate()
	return spark

def load_parquet_to_gcs(sdf: sdf, dataset: str):
	''' Saves a pandas-on-Spark DataFrame as a Parquet file to Google Cloud Storage.
	Args:
		df (ps.DataFrame): The DataFrame to save.
		dataset (str): Name of the dataset (without parquet extension).
	'''

	output_path = f"gs://dataengineering-internship-test-bucket/data/processed/{dataset}.parquet/"
	df = sdf.toPandas()
	df["user_id"] = df["user_id"].astype("Int32")
	df.to_parquet(output_path, partition_cols="partition_date", existing_data_behavior='overwrite_or_ignore')
	logging.info(f"Saved {dataset} data to {output_path}")

if __name__ == "__main__":
	logger = logging.getLogger()
	logger.setLevel(logging.INFO)
	gcs = os.getenv("GCS") == "True"
	logging.info(f"Read from gcs: {gcs}")
	spark = buildSparkSession(gcs)

	transactions_df = read_gcs_file("dataengineering-internship-test-bucket", "data/raw/transactions.csv", spark, gcs=gcs, schema="txn_id STRING, user_id INT, amount FLOAT, currency STRING, txn_time TIMESTAMP")
	clickstream_df = read_gcs_file("dataengineering-internship-test-bucket", "data/raw/clickstream.csv", spark, gcs=gcs, schema="user_id INT, session_id STRING, page_url STRING, click_time TIMESTAMP, device STRING, location STRING")

	transactions_df = clean_data(subset=["txn_id"], sdf=transactions_df, dataset="transactions")
	clickstream_df = clean_data(subset=["session_id", "click_time"], sdf=clickstream_df, dataset="clickstream")

	exchange_rates = ingest_currency_api()
	clickstream_df, transactions_df = transform(spark, exchange_rates, transactions_df, clickstream_df)

	load_parquet_to_gcs(clickstream_df, "clickstream")
	load_parquet_to_gcs(transactions_df, "transactions")
