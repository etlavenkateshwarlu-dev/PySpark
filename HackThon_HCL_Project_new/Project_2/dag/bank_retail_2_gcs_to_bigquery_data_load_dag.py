# File name : bank_retail_2_gcs_to_bigquery_data_load_dag.py
#
# Uses:
#   - DataprocSubmitJobOperator   (run PySpark cleansing)
#   - GCSObjectExistenceSensor   (wait for curated parquet)
#   - BigQueryCreateEmptyDatasetOperator
#   - GCSToBigQueryOperator      (for ALL BigQuery load tasks)

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


PROJECT_ID = "vctbatch-45"
REGION = "us-central1"
CLUSTER_NAME = "vctbatch-45-dataproc-cluster"

BUCKET = "hack-hcl-bank-retail_2"

PYSPARK_SCRIPT = "gs://hack-hcl-bank-retail_2/cleansing_data/pyspark_scripts/BankRetailCleansing.py"

DATASET_ID = "bank_retail_stage_ds1"

default_args = {
    "owner": "airflow",
    "depends_on_past": False
}

with DAG(
    dag_id="bank_retail_2_gcs_to_bigquery_data_load_dag",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False
) as dag:

    # ------------------------------------------------------------------
    # 1) Submit PySpark cleansing job
    # ------------------------------------------------------------------
    submit_cleansing_job = DataprocSubmitJobOperator(
        task_id="submit_bank_retail_cleansing_job",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": PYSPARK_SCRIPT
            }
        }
    )

    # ------------------------------------------------------------------
    # 2) Wait for curated parquet outputs
    # ------------------------------------------------------------------
    wait_for_branch = GCSObjectExistenceSensor(
        task_id="wait_for_branch_curated",
        bucket=BUCKET,
        object="cleansing_data/input_Branch_curated.parquet/_SUCCESS",
        poke_interval=30,
        timeout=900
    )

    wait_for_customer = GCSObjectExistenceSensor(
        task_id="wait_for_customer_curated",
        bucket=BUCKET,
        object="cleansing_data/input_Customer_curated.parquet/_SUCCESS",
        poke_interval=30,
        timeout=900
    )

    wait_for_product = GCSObjectExistenceSensor(
        task_id="wait_for_product_curated",
        bucket=BUCKET,
        object="cleansing_data/input_Product_curated.parquet/_SUCCESS",
        poke_interval=30,
        timeout=900
    )

    wait_for_transaction = GCSObjectExistenceSensor(
        task_id="wait_for_transaction_curated",
        bucket=BUCKET,
        object="cleansing_data/input_Transaction_curated.parquet/_SUCCESS",
        poke_interval=30,
        timeout=900
    )

    # ------------------------------------------------------------------
    # 3) Create BigQuery dataset
    # ------------------------------------------------------------------
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bank_retail_stage_dataset",
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        location="US",
        exists_ok=True
    )

    # ------------------------------------------------------------------
    # 4) Load curated parquet to BigQuery (GCSToBigQueryOperator)
    # ------------------------------------------------------------------
    load_branch = GCSToBigQueryOperator(
        task_id="load_branch_to_bq",
        bucket=BUCKET,
        source_objects=["cleansing_data/input_Branch_curated.parquet/*"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.branch",
        source_format="PARQUET",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED"
    )

    load_customer = GCSToBigQueryOperator(
        task_id="load_customer_to_bq",
        bucket=BUCKET,
        source_objects=["cleansing_data/input_Customer_curated.parquet/*"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.customer",
        source_format="PARQUET",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED"
    )

    load_product = GCSToBigQueryOperator(
        task_id="load_product_to_bq",
        bucket=BUCKET,
        source_objects=["cleansing_data/input_Product_curated.parquet/*"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.product",
        source_format="PARQUET",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED"
    )

    load_transaction = GCSToBigQueryOperator(
        task_id="load_transaction_to_bq",
        bucket=BUCKET,
        source_objects=["cleansing_data/input_Transaction_curated.parquet/*"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.transaction",
        source_format="PARQUET",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED"
    )

    # ------------------------------------------------------------------
    # Sequential dependency (single line)
    # ------------------------------------------------------------------
    submit_cleansing_job >> wait_for_branch >> wait_for_customer >> wait_for_product >> wait_for_transaction >> create_dataset >> load_branch >> load_customer >> load_product >> load_transaction
