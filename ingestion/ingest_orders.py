import os
import snowflake.connector
from pathlib import Path
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

# ----------------------------
# Load .env
# ----------------------------
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# ----------------------------
# Load private key
# ----------------------------
key_path = Path(__file__).parent.parent / "rsa_key.pem"

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

# ----------------------------
# Connect to Snowflake
# ----------------------------
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    private_key=pkb,
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)

# ----------------------------
# File path (local CSV)
# ----------------------------
file_path = Path(__file__).parent / "data/orders.csv"

# ----------------------------
# Execute ingestion
# ----------------------------
try:
    cs = conn.cursor()

    print("Uploading file to stage...")

    cs.execute(f"""
        PUT 'file://{file_path}' @OLIST_RAW.OLIST.ingestion_stage
        AUTO_COMPRESS=TRUE
        OVERWRITE=TRUE;
    """)

    print("Loading data into ORDERS table...")

    cs.execute("""
        COPY INTO OLIST_RAW.OLIST.ORDERS (
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date,
            loaded_at
        )
        FROM (
            SELECT
                t.$1,
                t.$2,
                t.$3,
                t.$4,
                t.$5,
                t.$6,
                t.$7,
                t.$8,
                CURRENT_TIMESTAMP()
            FROM @OLIST_RAW.OLIST.ingestion_stage t
        )
        FILE_FORMAT = (
            TYPE = CSV
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        )
        ON_ERROR = 'CONTINUE';
    """)

    print("Ingestion complete 🚀")

finally:
    cs.close()
    conn.close()