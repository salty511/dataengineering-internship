from google.cloud import storage

if __name__ == "__main__":
	storage_client = storage.Client()
	bucket = storage_client.bucket("dataengineering-internship-test-bucket")
	clickstream_blob = bucket.blob("clickstream_blob")
	transactions_blob = bucket.blob("transactions_blob")

	with open("data/cleaned/clickstream_utc.csv", "r") as f:
		clickstream_blob.upload_from_string(f.read(), content_type="text/csv")

	with open("data/cleaned/transactions_usd.csv", "r") as f:
		transactions_blob.upload_from_string(f.read(), content_type="text/csv")
