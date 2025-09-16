import pandas as pd
import os 
import requests 
import logging
logging.basicConfig(
	format='%(asctime)s - %(levelname)s - %(message)s',
	datefmt='%Y-%m-%d %H:%M:%S',
	level=logging.INFO
)
from dotenv import load_dotenv
load_dotenv()


def get_exchange_rates() -> dict:
	API_KEY = os.getenv("API_KEY")

	url = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD" 
	response = requests.get(url) 
	data = response.json() 

	if response.status_code == 200 and data["result"] == "success": 
			rates = data["conversion_rates"] 
			logging.info("Exchange rates fetched successfully.")
			return rates
	else: 
		logging.error("API Error:", data)
		return {}

if __name__ == "__main__":
	# Remove old cleaned file if it exists
	if os.path.exists("data/cleaned/transactions_usd.csv"):
		os.remove("data/cleaned/transactions_usd.csv")
		
	exchange_rates = get_exchange_rates()
	
	transactions_iter = pd.read_csv("data/raw/transactions.csv", chunksize=50000)
	rowCount = 0

	for chunk in transactions_iter:
		transactions_df = chunk 
		transactions_df["amount_usd"] = transactions_df.apply(
			lambda row: row["amount"] / exchange_rates[row["currency"]], axis=1
		)
		rowCount += len(transactions_df)
		transactions_df.to_csv("data/cleaned/transactions_usd.csv", mode='a', index=False, header=not os.path.exists("data/cleaned/transactions_usd.csv"))
	
	logging.info(f"Total rows processed (currency conversion): {rowCount}")

