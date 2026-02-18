# =====================================================================================
# Bank Retail – GCS → cleansing → curated parquet (Dataproc PySpark job)
# IMPORTANT:
#  - Designed for files where each line is fully quoted:
#    "col1,col2,col3"
# =====================================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName(
    "bank-retail-gcs-to-bq-cleansing"
).getOrCreate()


# -------------------------------------------------------------------------------------
# GCS paths
# -------------------------------------------------------------------------------------
FILES = {
    "branch": {
        "in":  "gs://hack-hcl-bank-retail_2/source_data/input_Branch.csv",
        "out": "gs://hack-hcl-bank-retail_2/cleansing_data/input_Branch_curated.parquet"
    },
    "customer": {
        "in":  "gs://hack-hcl-bank-retail_2/source_data/input_Customer.csv",
        "out": "gs://hack-hcl-bank-retail_2/cleansing_data/input_Customer_curated.parquet"
    },
    "product": {
        "in":  "gs://hack-hcl-bank-retail_2/source_data/input_Product.csv",
        "out": "gs://hack-hcl-bank-retail_2/cleansing_data/input_Product_curated.parquet"
    },
    "transaction": {
        "in":  "gs://hack-hcl-bank-retail_2/source_data/input_Transaction.csv",
        "out": "gs://hack-hcl-bank-retail_2/cleansing_data/input_Transaction_curated.parquet"
    }
}

EMAIL_REGEX = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'

ACCENT_MAP = {
    "ø":"o","Ø":"O","ë":"e","Ë":"E","å":"a","Å":"A","ö":"o","Ö":"O",
    "ä":"a","Ä":"A","é":"e","É":"E","è":"e","È":"E","ê":"e","Ê":"E",
    "á":"a","Á":"A","à":"a","À":"A","ó":"o","Ó":"O","ò":"o","Ò":"O",
    "í":"i","Í":"I","ì":"i","Ì":"I","ú":"u","Ú":"U","ù":"u","Ù":"U",
    "ñ":"n","Ñ":"N","ç":"c","Ç":"C","€":"EUR"
}


# -------------------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------------------
def normalize_accents(e):
    for k, v in ACCENT_MAP.items():
        e = regexp_replace(e, k, v)
    return e


def clean_column_names(df):
    for c in df.columns:
        new_c = c.strip().replace('"', '').replace("'", "")
        df = df.withColumnRenamed(c, new_c)
    return df


# -------------------------------------------------------------------------------------
# Common cleansing
# -------------------------------------------------------------------------------------
def common_cleansing(df):

    string_cols = [c for c, t in df.dtypes if t == "string"]

    for c in string_cols:
        e = trim(col(c))
        e = normalize_accents(e)
        e = regexp_replace(e, r'[\x00-\x1F\x7F]', '')
        e = regexp_replace(e, r'[^A-Za-z0-9@._,\- /]', '')
        df = df.withColumn(c, e)

    for c in string_cols:
        cl = c.lower()
        if "date" in cl or cl.endswith("dt"):
            df = df.withColumn(
                c,
                coalesce(
                    to_date(col(c), "yyyy-MM-dd"),
                    to_date(col(c), "dd-MM-yyyy"),
                    to_date(col(c), "MM/dd/yyyy"),
                    to_date(col(c))
                )
            )

    for c in string_cols:
        if "email" in c.lower():
            df = df.withColumn(
                c,
                when(col(c).rlike(EMAIL_REGEX), col(c)).otherwise(None)
            )

    return df


# -------------------------------------------------------------------------------------
# Generic datatype normalization
# -------------------------------------------------------------------------------------
def datatype_normalization(df):

    for c, t in df.dtypes:
        cl = c.lower()
        if t == "string" and (
            "amount" in cl or "price" in cl or "balance" in cl or
            "qty" in cl or "quantity" in cl
        ):
            df = df.withColumn(
                c,
                regexp_replace(col(c), ",", "").cast("double")
            )

    return df


# -------------------------------------------------------------------------------------
# ID standardization
# -------------------------------------------------------------------------------------
def standardize_ids(df):

    if "branch_id" in df.columns:
        df = df.withColumn(
            "branch_id",
            when(
                length(regexp_replace(col("branch_id"), "[^0-9]", "")) > 0,
                concat(
                    lit("BR_"),
                    lpad(regexp_replace(col("branch_id"), "[^0-9]", ""), 3, "0")
                )
            )
        )

    if "transaction_id" in df.columns:
        df = df.withColumn(
            "transaction_id",
            when(
                length(regexp_replace(col("transaction_id"), "[^0-9]", "")) > 0,
                concat(lit("TXN_"),
                       regexp_replace(col("transaction_id"), "[^0-9]", ""))
            )
        )

    if "account_id" in df.columns:
        df = df.withColumn(
            "account_id",
            when(
                length(regexp_replace(col("account_id"), "[^0-9]", "")) > 0,
                concat(lit("ACC_"),
                       regexp_replace(col("account_id"), "[^0-9]", ""))
            )
        )

    if "product_id" in df.columns:
        cleaned = regexp_replace(col("product_id"), "[^A-Za-z0-9]", "")
        df = df.withColumn(
            "product_id",
            when(
                cleaned.rlike("^[0-9]+$"),
                concat(lit("PROD_A"), lpad(cleaned, 3, "0"))
            ).otherwise(
                concat(lit("PROD_"), cleaned)
            )
        )

    return df


# -------------------------------------------------------------------------------------
# Null profiling
# -------------------------------------------------------------------------------------
def null_profile(df, tag):

    exprs = []
    for c, t in df.dtypes:
        if t == "string":
            exprs.append(
                sum(when(col(c).isNull() | (trim(col(c)) == ""), 1).otherwise(0)).alias(c)
            )
        else:
            exprs.append(
                sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
            )

    print(f"\n================ NULL PROFILE : {tag} ================")
    df.select(exprs).show(truncate=False)


# -------------------------------------------------------------------------------------
# Main pipeline
# -------------------------------------------------------------------------------------
for name, p in FILES.items():

    # Each row is fully quoted:
    # "a,b,c"
    df = (
        spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("sep", ",")
            .option("quote", "\u0000")
            .option("mode", "PERMISSIVE")
            .csv(p["in"])
    )

    df = clean_column_names(df)

    null_profile(df, f"{name}_before")

    df = common_cleansing(df)
    df = datatype_normalization(df)
    df = standardize_ids(df)

    null_profile(df, f"{name}_after")

    df.write.mode("overwrite").parquet(p["out"])


print("GCS → cleansing → curated parquet completed successfully.")

spark.stop()
