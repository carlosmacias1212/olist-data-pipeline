# Olist Cloud-Enabled ELT Pipeline (AWS S3 + Snowflake + dbt + Airflow)

This project simulates a production-style ELT pipeline using Python, AWS S3, Snowflake, dbt, and Airflow.  
Raw e-commerce data is ingested into an S3-based raw storage layer, loaded into Snowflake via an external stage, transformed into analytics-ready models with dbt, and orchestrated through Airflow with logging, retries, and automated testing.

---

## Architecture
```
Raw CSV Data
    ↓
Python Ingestion Script
    ↓
AWS S3 Raw Layer (partitioned file-based storage)
    ↓
Snowflake External Stage + COPY INTO
    ↓
Snowflake Raw Layer (OLIST_RAW.OLIST)
    ↓
dbt Transformations (staging → intermediate → marts)
    ↓
Analytics Layer (OLIST_ANALYTICS.DBT_CMACIAS)
    ↓
Airflow Orchestration (DAG)
    ↓
dbt Tests + Logging
```
---

## Tech Stack

- Python — ingestion scripts, pipeline logic, and S3 integration  
- AWS S3 — raw file-based storage layer for scalable data ingestion  
- Snowflake — cloud data warehouse (raw + analytics layers, external stage loading)  
- dbt Core — transformations, testing, and incremental models  
- Airflow — workflow orchestration, scheduling, retries, and dependencies  
- dotenv / environment variables — configuration management  

---

## 📁 Repository Structure
```
olist-data-pipeline/
├── ingestion/
│   ├── ingest_data.py         #Generic ingestion script
│   ├── config.py              #Table-specific configs
│   └── data/                  #Source CSV files
│
├── dbt/
│   └── olist_dbt_project/     #dbt project (staging, intermediate, marts)
│
├── orchestration/
│   └── airflow/
│       └── olist_pipeline.py  #Airflow DAG
│
├── .env                       #Environment variables (not committed)
├── requirements.txt
└── README.md
```
---

## Pipeline Flow

1. Ingestion  
   - Python script uploads CSV files to AWS S3 using a partitioned structure (`load_date`)  
   - Data is stored in a raw file-based storage layer (data lake pattern)  
   - Snowflake loads data using an external stage and `COPY INTO`  
   - A `loaded_at` timestamp is added for incremental processing 

2. Transformation  
   - dbt builds:
     - staging models (cleaned raw data)
     - intermediate models (joins + business logic)
     - fact/dimension tables (analytics layer)

3. Incremental Processing  
   - dbt models use loaded_at to process only new data  
   - avoids full refreshes and improves efficiency  

4. Orchestration  
   - Airflow DAG runs:
     - ingestion
     - dbt run
     - dbt test  
   - includes retries and task dependencies  

5. Validation  
   - dbt tests validate data quality  
   - ingestion logs track row counts and execution  

---

## Key Features

- Generic ingestion framework  
  - Single script supports multiple tables via configuration  

- Cloud-based ingestion architecture  
  - Uses AWS S3 as a raw storage layer  
  - Loads data into Snowflake via external stage and `COPY INTO`  

- Partitioned file-based storage  
  - Organizes data in S3 using `load_date` for scalable ingestion and incremental processing  

- Incremental dbt models  
  - Processes only new data using loaded_at  

- Airflow orchestration  
  - End-to-end pipeline automation  
  - Task dependencies and retries  

- Structured logging  
  - Logs ingestion steps and row counts  
  - Integrated into Airflow task logs  

- Data quality checks  
  - dbt tests validate transformations  

---

## ▶️ How to Run

### 1. Clone repo

git clone https://github.com/carlosmacias1212/olist-data-pipeline.git  
cd olist-data-pipeline  

---

### 2. Set up virtual environment

python3 -m venv venv  
source venv/bin/activate  

---

### 3. Install dependencies

pip install -r requirements.txt  

---

### 4. Configure environment variables

Create a `.env` file:

SNOWFLAKE_USER=your_user  
SNOWFLAKE_ACCOUNT=your_account  
SNOWFLAKE_WAREHOUSE=your_warehouse  
SNOWFLAKE_DATABASE=OLIST_RAW  
SNOWFLAKE_SCHEMA=OLIST  
PRIVATE_KEY_PATH=rsa_key.pem  

---

### 5. Run ingestion manually

python ingestion/ingest_data.py orders  

---

### 6. Run dbt

cd dbt  
dbt run  
dbt test  

---

### 7. Run full pipeline (Airflow)

export AIRFLOW_HOME=~/airflow  
airflow standalone  

Then open:

http://localhost:8080  

Trigger the DAG:

olist_pipeline  

---

## Example Outputs

- Airflow DAG run with all tasks successful  
- dbt models materialized in Snowflake  
- Ingestion logs showing row counts and execution steps  

---

## Example Log Output

INFO | Starting ingestion for table: orders  
INFO | Uploading file to S3...
INFO | Loading data from external stage...  
INFO | Loading data into table...  
INFO | Total rows in orders: 99,445  
INFO | orders ingestion complete 🚀  

---
## Data Storage Design

The pipeline uses a file-based storage pattern in AWS S3 to simulate a modern data lake architecture.

Example structure:
s3://bucket-name/raw/orders/load_date=2026-04-17/orders.csv
s3://bucket-name/raw/customers/load_date=2026-04-17/customers.csv

This structure enables:
- scalable ingestion patterns  
- partition-based processing  
- separation of storage and compute  

## Key Learnings

- Built a full ELT pipeline from ingestion to analytics layer
- Implemented a cloud-based ingestion layer using AWS S3 and Snowflake external stages  
- Applied file-based storage and partitioning concepts to simulate data lake architecture  
- Implemented incremental data processing for efficiency  
- Orchestrated workflows using Airflow with retries and dependencies  
- Added logging and validation to improve observability  
- Designed scalable ingestion using configuration-driven approach  

---

## Future Improvements

- Add alerting (Slack/email) for failed pipelines  
- Containerize with Docker  
- Replace AWS key-based access with IAM roles / Snowflake storage integration  
- Add data freshness monitoring  
- Integrate with external APIs instead of static CSVs  

---

## Summary

This project demonstrates the core responsibilities of a data engineer:

- moving data reliably  
- transforming it into usable models  
- orchestrating workflows  
- ensuring data quality and observability  
