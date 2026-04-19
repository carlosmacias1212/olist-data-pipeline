import os
import sys
import logging
from datetime import datetime
from pathlib import Path

import boto3
import snowflake.connector
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

from config import TABLE_CONFIG

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# Load env
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

database = os.getenv("SNOWFLAKE_DATABASE")
schema = os.getenv("SNOWFLAKE_SCHEMA")
project_root = Path(__file__).parent.parent

# AWS / S3 config
s3_bucket = os.getenv("S3_BUCKET_NAME")
aws_region = os.getenv("AWS_REGION")
snowflake_external_stage = os.getenv("SNOWFLAKE_EXTERNAL_STAGE")

# Snowflake key path
key_path = project_root / os.getenv("PRIVATE_KEY_PATH")

# Get table name from CLI
if len(sys.argv) < 2:
    raise ValueError("Please provide a table name (e.g. orders)")

table_name = sys.argv[1]

if table_name not in TABLE_CONFIG:
    logger.error(f"Invalid table name: {table_name}")
    raise ValueError(f"Table '{table_name}' not found in config")

config = TABLE_CONFIG[table_name]
logger.info(f"Starting ingestion for table: {table_name}")

# Load private key
logger.info("Loading private key...")
with open(key_path, "rb") as key:
    p_key = serialization.load_pem_private_key(
        key.read(),
        password=None,
    )

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)

# Connect to Snowflake
logger.info("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    private_key=pkb,
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=database,
    schema=schema,
)
cs = conn.cursor()

# Create S3 client
s3_client = boto3.client(
    "s3",
    region_name=aws_region,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

try:
    file_path = project_root / "ingestion" / config["file_path"]
    columns = config["columns"]
    s3_key_prefix = config["s3_key_prefix"]

    load_date = datetime.today().strftime("%Y-%m-%d")
    file_name = Path(config["file_path"]).name
    s3_key = f"{s3_key_prefix}/load_date={load_date}/{file_name}"

    logger.info(f"Uploading file to S3: s3://{s3_bucket}/{s3_key}")

    # Upload local file to S3
    s3_client.upload_file(str(file_path), s3_bucket, s3_key)

    logger.info("File uploaded to S3 successfully")

    # Build Snowflake stage reference
    stage_path = f"{snowflake_external_stage}/{s3_key}"

    # Build column list
    column_list = ", ".join(columns + ["loaded_at"])

    # Build select statement dynamically
    select_cols = ",\n".join([f"t.${i+1}" for i in range(len(columns))])
    select_cols += ",\nCURRENT_TIMESTAMP()"

    logger.info(f"Loading file from external stage: {stage_path}")

    cs.execute(f"""
        COPY INTO {database}.{schema}.{table_name} ({column_list})
        FROM (
            SELECT
                {select_cols}
            FROM {stage_path} t
        )
        FILE_FORMAT = (
            TYPE = CSV
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        )
        ON_ERROR = 'CONTINUE';
    """)

    cs.execute(f"SELECT COUNT(*) FROM {database}.{schema}.{table_name}")
    row_count = cs.fetchone()[0]
    logger.info(f"Total rows in {table_name}: {row_count}")
    logger.info(f"{table_name} ingestion complete 🚀")

except Exception as e:
    logger.error(f"Ingestion failed for {table_name}: {str(e)}")
    raise

finally:
    logger.info("Closing Snowflake connection")
    cs.close()
    conn.close()