# AWS Serverless E-Commerce ETL Pipeline

A serverless data engineering pipeline that processes raw e-commerce event data using AWS S3, Lambda, Glue, PySpark, and Athena.

## Architecture

S3 (Raw Data)
        │
        ▼
AWS Lambda
(S3 event trigger)
        │
        ▼
AWS Glue
(PySpark ETL)
        │
        ▼
S3 (Processed Data)
(Partitioned Parquet)
        │
        ▼
AWS Glue Crawler
        │
        ▼
Glue Data Catalog
        │
        ▼
Amazon Athena
(SQL Analytics)


## Project Overview

The pipeline processes raw JSON e-commerce event data and converts it into optimized Parquet files suitable for analytical queries.

The pipeline automatically starts when a new JSON file is uploaded to the raw S3 bucket.

### Processing flow

1. Raw JSON data is uploaded to Amazon S3.
2. An S3 event triggers AWS Lambda.
3. Lambda starts the AWS Glue ETL job.
4. Glue runs a PySpark transformation job.
5. The processed data is written to S3 as Parquet.
6. Data is partitioned by year and month.
7. A Glue Crawler discovers the schema and partitions.
8. The Glue Data Catalog stores the table metadata.
9. Amazon Athena queries the processed Parquet data using SQL.


## AWS Services Used

| Service | Purpose |
|---|---|
| Amazon S3 | Stores raw and processed data |
| AWS Lambda | Triggers the ETL pipeline when new data arrives |
| AWS Glue | Runs the PySpark ETL job |
| PySpark | Cleans and transforms the data |
| Glue Crawler | Discovers schema and partitions |
| Glue Data Catalog | Stores table metadata |
| Amazon Athena | Performs SQL analytics on processed data |
| IAM | Controls permissions between AWS services |


## Data Transformations

The PySpark job performs the following transformations:

### 1. Timestamp conversion

Converts `event_time` from a string into a timestamp.

```python
to_timestamp(
    col("event_time"),
    "yyyy-MM-dd HH:mm:ss 'UTC'"
)
```

### 2. Null handling

Missing `brand` values are replaced with `"Unknown"`.

```python
when(col("brand").isNull(), "Unknown") \
    .otherwise(col("brand"))
```

### 3. Deduplication

Duplicate events are removed using a composite key consisting of:

- `event_time`
- `event_type`
- `product_id`
- `user_id`
- `user_session`

```python
df.dropDuplicates([
    "event_time",
    "event_type",
    "product_id",
    "user_id",
    "user_session"
])
```

### 4. Processing timestamp

A `processed_at` column records when the data was processed.

```python
current_timestamp()
```

### 5. Partitioning

The event timestamp is used to create year and month partitions.

```text
year=2020/
    month=1/
    month=2/
```

Partitioning allows Athena to scan only relevant partitions when filtering by date.

### 6. Format conversion

Raw JSON is converted into Parquet for more efficient analytical querying.
Sample Input

A small sample dataset is included in:

sample_data/sample.json

The original dataset used during development was much larger and was stored in Amazon S3 rather than GitHub.

Repository Structure
aws-serverless-etl-pipeline/
│
├── lambda/
│   └── lambda_function.py
│
├── glue/
│   └── order_etl.py
│
├── sample_data/
│   └── sample.json
│
└── README.md
Key Concepts Learned

This project demonstrates practical experience with:

Serverless ETL
Event-driven pipelines
AWS S3
AWS Lambda
AWS Glue
PySpark
JSON processing
Data cleaning
Deduplication
Parquet
Data partitioning
Glue Data Catalog
Glue Crawlers
Amazon Athena
SQL analytics
IAM permissions
Passing parameters between AWS services

