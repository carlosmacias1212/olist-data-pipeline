# Olist End-to-End ELT Pipeline

This project simulates a production-style ELT pipeline using Python, Snowflake, dbt, and Airflow.  
Raw e-commerce data is ingested into Snowflake, transformed into analytics-ready models with dbt, and orchestrated through Airflow with logging, retries, and automated testing.

---

## Architecture

Raw CSV Data <br>
    ↓<br>
Python Ingestion (PUT + COPY INTO)<br>
    ↓<br>
Snowflake Raw Layer (OLIST_RAW.OLIST)<br>
    ↓
dbt Transformations (staging → intermediate → marts)
    ↓
Analytics Layer (OLIST_ANALYTICS.DBT_CMACIAS)
    ↓
Airflow Orchestration (DAG)
    ↓
dbt Tests + Logging

---

## Tech Stack

- Python — ingestion scripts and pipeline logic  
- Snowflake — cloud data warehouse (raw + analytics layers)  
- dbt Core — transformations, testing, and incremental models  
- Airflow — workflow orchestration and scheduling  
- dotenv / environment variables — configuration management  

---

## 📁 Repository Structure

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

---

## Pipeline Flow

1. Ingestion  
   - Python script uploads CSV files to Snowflake using PUT  
   - Data is loaded using COPY INTO  
   - A loaded_at timestamp is added for incremental processing  

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

- Snowflake-native loading  
  - Uses PUT + COPY INTO pattern  

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
INFO | Uploading file to stage...  
INFO | Loading data into table...  
INFO | Total rows in orders: 99,445  
INFO | orders ingestion complete 🚀  

---

## Key Learnings

- Built a full ELT pipeline from ingestion to analytics layer  
- Implemented incremental data processing for efficiency  
- Orchestrated workflows using Airflow with retries and dependencies  
- Added logging and validation to improve observability  
- Designed scalable ingestion using configuration-driven approach  

---

## Future Improvements

- Add alerting (Slack/email) for failed pipelines  
- Containerize with Docker  
- Deploy Airflow in a cloud environment  
- Add data freshness monitoring  
- Integrate with external APIs instead of static CSVs  

---

## Summary

This project demonstrates the core responsibilities of a data engineer:

- moving data reliably  
- transforming it into usable models  
- orchestrating workflows  
- ensuring data quality and observability  
