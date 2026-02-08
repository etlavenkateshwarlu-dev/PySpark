from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitPySparkJobOperator
)

# ---------------------------------------------------------
# Change only these values for your environment
# ---------------------------------------------------------
PROJECT_ID   = "vctbatch-45"
REGION       = "us-central1"
CLUSTER_NAME = "vctbatch-45-dataproc-cluster"

# Your PySpark file already uploaded to GCS
PYSPARK_URI  = "gs://hack-hcl-bank-retail/pyspark_scripts/BankRetailDataClensing.py"

# Optional jars (only needed if you use special connectors)
JARS = []

with DAG(
    dag_id="BankRetailDataClensingDAG",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    run_pyspark_job = DataprocSubmitPySparkJobOperator(
        task_id="run_customers_cleansing_job",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        main=PYSPARK_URI,      # <<<<<< PySpark file from GCS
        jars=JARS,

    )

    run_pyspark_job
