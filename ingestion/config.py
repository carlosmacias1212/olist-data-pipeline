TABLE_CONFIG = {
    "orders": {
        "columns": [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        "stage": "@OLIST_RAW.OLIST.ingestion_stage",
        "file_path": "data/orders.csv",
    },
    "customers": {
        "columns": [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
        "stage": "@OLIST_RAW.OLIST.ingestion_stage",
        "file_path": "data/customers.csv",
    }
}