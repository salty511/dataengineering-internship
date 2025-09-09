import pandas as pd
import os

if __name__ == "__main__":
	# Remove old cleaned file if it exists
	if os.path.exists("data/cleaned/clickstream_utc.csv"):
		os.remove("data/cleaned/clickstream_utc.csv")

	clickstream_iter = pd.read_csv("data/raw/clickstream.csv", chunksize=50000)

	for chunk in clickstream_iter:
		clickstream_df = chunk 
		clickstream_df["click_time_utc"] = pd.to_datetime(clickstream_df["click_time"], utc=True)
		clickstream_df.to_csv("data/cleaned/clickstream_utc.csv", mode='a', index=False, header=not os.path.exists("data/cleaned/clickstream_utc.csv"))