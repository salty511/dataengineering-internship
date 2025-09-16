# Data Engineering Intership

This is a project I worked on as part of a data engineering internship.

## Data Exploration

The data for this project is contained within `data/raw/` and represents clickstream and transaction data from an e-commerce business. Clickstream data records information about a user when they navigate to a specific url i.e. `/checkout` and transactions data records information about transactions made on the website. 

### Schema

#### Clickstream

```json
clickstream: {
	user_id: Int,
	session_id: String,
	page_url: {
		type: String,
		values: [
			'/home', '/product/alpha', '/checkout', '/product/beta', '/cart', '/', 
			'/product/gamma', '/search?q=data', '/docs/getting-started', '/pricing'
		]
	},
	click_time: Date,
	device: {
		type: String,
		values: ['desktop', 'mobile', 'tablet']
	}
	location: {
		type: String,
		values: ['DE', 'AU', 'GB', 'ZA', 'BR', 'JP', 'CA', 'FR', 'US', 'IN']
	}
}
```

No duplicates on session_id\
No null values

#### Transactions

```json
transactions: {
	txn_id: String,
	user_id: Int,
	amount: Float,
	currency: {
		type: String,
		values: ['GBP', 'USD', 'EUR', 'INR', 'JPY']
	},
	txn_time: Date
}
```

No duplicates on txn_id\
No null values

## ETL Pipeline

The ETL pipeline extracts data from local and remote sources, performs cleaning and transformation operations and loads the data into GCS.

### Extraction

The data for this project are contained within local csv files in `data/raw/` however, we also need to get currency conversion rates for the transactions

Sources: data/raw/*\
ETL Scripts: src/*\
Store: GCS

![alt text](media/pipeline-diagram.png "Pipeline Diagram")

## GCS Bucket

![alt text](media/gcs-bucket-screenshot.png "GCS Bucket screenshot")