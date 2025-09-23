import os
from pyspark.sql import SparkSession
import pyspark.pandas as ps
import sys
from dotenv import load_dotenv
load_dotenv()

def remove_file_if_exists(file_path: str) -> None:
	if os.path.exists(file_path):
		os.remove(file_path)

def clean_data(subset: list, dataset: str, DATA_DIR=os.getenv('DATA_DIR')) -> ps.DataFrame:
	''' Cleans the specified dataset by removing duplicates and null values.
	Args:
		subset (list): List of columns to check for duplicates.
		dataset (str): Name of the dataset (without .csv extension).
		DATA_DIR (str): Directory where data is stored.
	Returns:
		str: Summary of cleaning operation.
	'''

	# Remove old cleaned file if it exists
	remove_file_if_exists(f"{DATA_DIR}/cleaned/{dataset}.csv")

	rowCount = 0
	duplicates = 0
	nulls = 0

	df = ps.read_csv(f"{DATA_DIR}/raw/{dataset}.csv")


	# Clean data
	df.dropna(inplace=True)
	df.drop_duplicates(subset=subset, inplace=True)
	
	# Track stats
	duplicates += df[subset].duplicated().sum()
	nulls += df.isnull().sum().sum()
	rowCount += len(df)

	# Write to cleaned file
	df.to_csv(f"{DATA_DIR}/cleaned/{dataset}.csv", mode='a', index=False, header=not os.path.exists(f"{DATA_DIR}/cleaned/{dataset}.csv"))

	print(f'''{dataset.capitalize()} data cleaned and saved to {DATA_DIR}/cleaned/{dataset}.csv:
		Rows processed: {rowCount}
		Duplicates on {subset}: {duplicates}
		Null values: {nulls}
	''')
	return df

def buildSparkSession():
	# Set JAVA_HOME for Spark
	os.environ['JAVA_HOME'] = "C:/Program Files/Java/jdk-21"
	

	conf = {
		"spark.driver.extraJavaOptions": "-Djava.security.manager=allow",
		"spark.executor.extraJavaOptions": "-Djava.security.manager=allow",
		"spark.sql.ansi.enabled": "false"
	}

	spark = SparkSession.builder.appName("DataCleaningApp").config(map=conf).getOrCreate()

if __name__ == "__main__":
	import json
	with open('sparkconf.json', 'r') as f:
		conf = json.load(f)

	os.environ['PYSPARK_PYTHON'] = sys.executable
	os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

	spark = SparkSession.builder\
		.appName("testApp").config(map=conf).getOrCreate()
	
	clean_data(subset=["txn_id"], dataset="transactions")