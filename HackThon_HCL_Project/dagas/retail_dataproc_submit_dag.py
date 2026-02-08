from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitPySparkJobOperator
)

PROJECT_ID   = "vctbatch-45"
REGION       = "us-central1"          # change if your cluster is in another region
CLUSTER_NAME = "vctbatch-45-dataproc-cluster"

PYSPARK_FILE = "gs://hack-hcl-bank-retail/pyspark_scripts/retail_cleansing_job.py"

with DAG(
    dag_id="retail_submit_pyspark_to_dataproc",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["dataproc", "pyspark", "retail"],
) as dag:

    submit_retail_cleansing = DataprocSubmitPySparkJobOperator(
        task_id="submit_retail_cleansing_job",

        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,

        main=PYSPARK_FILE,
    )

    submit_retail_cleansing
