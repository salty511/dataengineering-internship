from google.cloud import bigquery

#txn_id STRING, user_id INT, amount FLOAT, currency STRING, txn_time TIMESTAMP
if __name__ == "__main__":
	# Construct a BigQuery client object.
	client = bigquery.Client()
	hive_partition_opts = bigquery.HivePartitioningOptions()
	hive_partition_opts.mode = "AUTO"
	

	table_id_txn = "dataengineering-internship.bigquery_testing.transactions"
	hive_partition_opts.source_uri_prefix = "gs://dataengineering-internship-test-bucket/data/processed/transactions.parquet/"
	txn_schema = client.schema_from_json("schemas/transactions.json")

	job_config_txn = bigquery.LoadJobConfig(
		source_format=bigquery.SourceFormat.PARQUET,
		hive_partitioning=hive_partition_opts,
		write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
		schema=txn_schema
	)

	table_id_cs = "dataengineering-internship.bigquery_testing.clickstream"
	hive_partition_opts.source_uri_prefix = "gs://dataengineering-internship-test-bucket/data/processed/clickstream.parquet/"
	cs_schema = client.schema_from_json("schemas/clickstream.json")

	job_config_cs = bigquery.LoadJobConfig(
		source_format=bigquery.SourceFormat.PARQUET,
		hive_partitioning=hive_partition_opts,
		write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
		schema=cs_schema
	)

	uri_txn = "gs://dataengineering-internship-test-bucket/data/processed/transactions.parquet/*"
	uri_cs = "gs://dataengineering-internship-test-bucket/data/processed/clickstream.parquet/*"

	load_job_txn = client.load_table_from_uri(
		uri_txn, table_id_txn, job_config=job_config_txn
	)  # Make an API request.

	load_job_txn.result()  # Waits for the job to complete.

	destination_table = client.get_table(table_id_txn)  # Make an API request.
	print(f"Loaded {destination_table.num_rows} rows to {destination_table}")

	load_job_cs = client.load_table_from_uri(
		uri_cs, table_id_cs, job_config=job_config_cs
	)  # Make an API request.

	load_job_cs.result()  # Waits for the job to complete.

	destination_table = client.get_table(table_id_cs)  # Make an API request.
	print(f"Loaded {destination_table.num_rows} rows to {destination_table}")