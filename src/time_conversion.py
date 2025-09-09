import pandas as pd
import os

if __name__ == "__main__":
	clickstream_iter = pd.read_csv("data/clickstream.csv", chunksize=50000)

	for chunk in clickstream_iter:
		clickstream_df = chunk 
		clickstream_df["click_time_utc"] = pd.to_datetime(clickstream_df["click_time"], utc=True)
		clickstream_df.to_csv("data/clickstream_utc.csv", mode='a', index=False, header=not os.path.exists("data/clickstream_utc.csv"))