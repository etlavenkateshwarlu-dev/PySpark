from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


PROJECT_ID = "vctbatch-45"
REGION = "us-central1"          # change if your cluster is in another region
CLUSTER_NAME = "vctbatch-45-dataproc-cluster"
PYSPARK_FILE = "gs://hack-hcl-bank-retail/pyspark_scripts/bank_retail_cleansing_data.py"
BUCKET = "hack-hcl-bank-retail"
PREFIX = "cleansing_data/"
DATASET_ID = "bank_retail_stage_ds"
default_args = {
    "owner": "vctbatch-45",
    "depends_on_past": False,
    "retries":2,
    "retry_delay": 300,          # seconds (5 min)
    "email_on_failure": False,
    "email_on_retry": False,
}
dag=DAG(
    dag_id="bank_retail_gcs_to_bigquery_data_load_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["dataproc", "pyspark", "retail"],
)
submit_retail_cleansing_task = DataprocSubmitJobOperator(
    task_id="submit_retail_cleansing_task",
    project_id=PROJECT_ID,
    region=REGION,
    job={
        "placement": {
            "cluster_name": CLUSTER_NAME
        },
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_FILE
        }
    },
    dag=dag
    )
wait_for_files_task = GCSObjectsWithPrefixExistenceSensor(
    task_id="wait_for_files_task",
    bucket=BUCKET,
    prefix=PREFIX,
    # how often to check (seconds)
    poke_interval=60,
    # total waiting time (seconds)
    timeout=60 * 60,
    mode="poke",
    dag=dag
)

# -------------------------------------------------------
# Create dataset if not exists
# -------------------------------------------------------
create_retail_dataset_task = BigQueryCreateEmptyDatasetOperator(
    task_id="create_retail_dataset_task",
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    location="US",
    exists_ok=True,
    dag=dag
)

# -------------------------------------------------------
# Load customers (PARQUET)
# -------------------------------------------------------
load_customers_task = GCSToBigQueryOperator(
    task_id="load_customers_task",
    bucket=BUCKET,
    source_objects=["cleansing_data/Customers_curated.parquet/*"],
    destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.customers",
    source_format="PARQUET",
    write_disposition="WRITE_APPEND",
    create_disposition="CREATE_IF_NEEDED",
    autodetect=True,
    dag=dag
)

# -------------------------------------------------------
# Load locations (PARQUET)
# -------------------------------------------------------
load_locations_task = GCSToBigQueryOperator(
    task_id="load_locations_task",
    bucket=BUCKET,
    source_objects=["cleansing_data/Locations_curated.parquet/*"],
    destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.locations",
    source_format="PARQUET",
    write_disposition="WRITE_APPEND",
    create_disposition="CREATE_IF_NEEDED",
    autodetect=True,
    dag=dag
)

# -------------------------------------------------------
# Load Merchants (PARQUET)
# -------------------------------------------------------
load_merchants_task = GCSToBigQueryOperator(
    task_id="load_merchants_task",
    bucket=BUCKET,
    source_objects=["cleansing_data/Merchants_curated.parquet/*"],
    destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.merchants",
    source_format="PARQUET",
    write_disposition="WRITE_APPEND",
    create_disposition="CREATE_IF_NEEDED",
    autodetect=True,
    dag=dag
)

# -------------------------------------------------------
# Load Time (PARQUET)
# -------------------------------------------------------
load_time_task = GCSToBigQueryOperator(
    task_id="load_time_task",
    bucket=BUCKET,
    source_objects=["cleansing_data/Time_curated.parquet/*"],
    destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.time",
    source_format="PARQUET",
    write_disposition="WRITE_APPEND",
    create_disposition="CREATE_IF_NEEDED",
    autodetect=True,
    dag=dag
)

# -------------------------------------------------------
# Load Transactions (PARQUET)
# -------------------------------------------------------
load_transactions_task = GCSToBigQueryOperator(
    task_id="load_transactions_task",
    bucket=BUCKET,
    source_objects=["cleansing_data/Transactions_curated.parquet/*"],
    destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.transactions",
    source_format="PARQUET",
    write_disposition="WRITE_APPEND",
    create_disposition="CREATE_IF_NEEDED",
    autodetect=True,
    dag=dag
)

submit_retail_cleansing_task >> wait_for_files_task >> create_retail_dataset_task

create_retail_dataset_task >> load_customers_task >> load_locations_task >> load_merchants_task >> load_time_task >> load_transactions_task
