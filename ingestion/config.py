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
        "file_path": "data/orders.csv",
        "s3_key_prefix": "raw/orders",
    },
    "customers": {
        "columns": [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
        "file_path": "data/customers.csv",
        "s3_key_prefix": "raw/customers",
    }
}