import subprocess

tables = ["orders", "customers"]

# Run ingestion
for table in tables:
    subprocess.run(["python", "ingestion/ingest_data.py", table], check=True)

# Run dbt
subprocess.run(["dbt", "run"], cwd="dbt", check=True)

print("Pipeline complete 🚀")