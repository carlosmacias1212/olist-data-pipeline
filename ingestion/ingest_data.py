import os
import sys
import snowflake.connector
from config import TABLE_CONFIG
from dotenv import load_dotenv
from pathlib import Path
from cryptography.hazmat.primitives import serialization
import logging

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

logger.info("Loading private key...")

# Load private key
with open("rsa_key.pem", "rb") as key:
    p_key = serialization.load_pem_private_key(
        key.read(),
        password=None,
    )

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)

logger.info("Connecting to Snowflake...")

# Connect
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    private_key=pkb,
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=database,
    schema=schema,
)

cs = conn.cursor()

try:
    file_path = project_root / "ingestion" / config["file_path"]
    stage = config["stage"]
    columns = config["columns"]

    logger.info(f"Uploading file to stage: {file_path}")

    # PUT (upload file)
    cs.execute(f"""
        PUT 'file://{file_path}' {stage}
        AUTO_COMPRESS=TRUE
        OVERWRITE=TRUE;
    """)

    logger.info("File uploaded successfully")

    # Build column list
    column_list = ", ".join(columns + ["loaded_at"])

    # Build select statement dynamically
    select_cols = ",\n".join([f"t.${i+1}" for i in range(len(columns))])
    select_cols += ",\nCURRENT_TIMESTAMP()"

    # COPY INTO
    cs.execute(f"""
        COPY INTO {database}.{schema}.{table_name} ({column_list})
        FROM (
            SELECT
                {select_cols}
            FROM {stage} t
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