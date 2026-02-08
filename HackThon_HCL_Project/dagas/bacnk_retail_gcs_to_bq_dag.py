# ============================================================
# Airflow DAG
# Project  : vctbatch-45
# Purpose  :
#   - Create BigQuery dataset if not exists
#   - Load all cleansed parquet data from GCS
#   - Load customers_cure.csv from GCS
# ============================================================

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator
)

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator
)

PROJECT_ID = "vctbatch-45"
DATASET_ID = "retail_dw"
BUCKET = "hack-hcl-bank-retail"

with DAG(
    dag_id="retail_load_gcs_to_bigquery_vctbatch_45",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["gcs", "bigquery", "retail"],
) as dag:

    # -------------------------------------------------------
    # Create dataset if not exists
    # -------------------------------------------------------
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_retail_dataset",
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        location="US",
        exists_ok=True,
    )

    # -------------------------------------------------------
    # Load Locations (PARQUET)
    # -------------------------------------------------------
    load_locations = GCSToBigQueryOperator(
        task_id="load_dim_locations",
        bucket=BUCKET,
        source_objects=["clensing_data/locations/*"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.dim_locations",
        source_format="PARQUET",
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
    )

    # -------------------------------------------------------
    # Load Merchants (PARQUET)
    # -------------------------------------------------------
    load_merchants = GCSToBigQueryOperator(
        task_id="load_dim_merchants",
        bucket=BUCKET,
        source_objects=["clensing_data/merchants/*"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.dim_merchants",
        source_format="PARQUET",
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
    )

    # -------------------------------------------------------
    # Load Time (PARQUET)
    # -------------------------------------------------------
    load_time = GCSToBigQueryOperator(
        task_id="load_dim_time",
        bucket=BUCKET,
        source_objects=["clensing_data/time/*"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.dim_time",
        source_format="PARQUET",
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
    )

    # -------------------------------------------------------
    # Load Transactions (PARQUET)
    # -------------------------------------------------------
    load_transactions = GCSToBigQueryOperator(
        task_id="load_fact_transactions",
        bucket=BUCKET,
        source_objects=["clensing_data/transactions/*"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.fact_transactions",
        source_format="PARQUET",
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
    )

    # -------------------------------------------------------
    # Load Customers (CSV)
    # -------------------------------------------------------
    load_customers = GCSToBigQueryOperator(
        task_id="load_dim_customers",
        bucket=BUCKET,
        source_objects=["source_data/customers_cure.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.dim_customers",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
    )

    create_dataset >> [
        load_locations,
        load_merchants,
        load_time,
        load_transactions,
        load_customers
    ]
