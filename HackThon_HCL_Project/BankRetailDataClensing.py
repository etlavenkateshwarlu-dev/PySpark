# ---------------------------------------------
# PySpark data cleansing for Customers.csv
# ---------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, initcap, when, regexp_extract
)

spark = SparkSession.builder.appName("customers-cleansing").getOrCreate()

# ------------------------------------------------
# 1. Read CSV
# ------------------------------------------------
df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv("gs://hack-hcl-bank-retail/source_data/Customers.csv")
)

# ------------------------------------------------
# 2. Basic cleansing operations
# ------------------------------------------------
# What we normally do:
#  - trim strings
#  - standardize casing
#  - validate codes
#  - filter bad records
#  - remove duplicates

clean_df = (
    df

    # ---- trim all string columns we care about
    .withColumn("CustomerID", trim(col("CustomerID")))
    .withColumn("CustomerName", trim(col("CustomerName")))
    .withColumn("Segment", trim(col("Segment")))
    .withColumn("HomeLocationID", trim(col("HomeLocationID")))
    .withColumn("PreferredChannel", trim(col("PreferredChannel")))

    # ---- standardize values
    # Name in proper case
    .withColumn("CustomerName", initcap(col("CustomerName")))

    # Segment in upper case
    .withColumn("Segment", upper(col("Segment")))

    # PreferredChannel in upper case
    .withColumn("PreferredChannel", upper(col("PreferredChannel")))
)

# ------------------------------------------------
# 3. Validate business rules
# ------------------------------------------------
# Example rules:
#  - CustomerID must start with 'C' and digits
#  - Segment must be one of: STUDENT, PREMIUM, STANDARD
#  - PreferredChannel must be one of: DIGITAL, ATM, BRANCH

valid_df = (
    clean_df
    .filter(col("CustomerID").rlike("^C[0-9]+$"))
    .filter(col("Segment").isin("STUDENT", "PREMIUM", "STANDARD"))
    .filter(col("PreferredChannel").isin("DIGITAL", "ATM", "BRANCH"))
)

# ------------------------------------------------
# 4. Handle duplicates (business key = CustomerID)
# ------------------------------------------------
dedup_df = valid_df.dropDuplicates(["CustomerID"])

# ------------------------------------------------
# 5. Example of standardizing / fixing bad values
# (If future files contain variations like "Atm", "atm", etc.)
# ------------------------------------------------
final_df = (
    dedup_df
    .withColumn(
        "PreferredChannel",
        when(col("PreferredChannel").like("ATM%"), "ATM")
        .when(col("PreferredChannel").like("DIGITAL%"), "DIGITAL")
        .when(col("PreferredChannel").like("BRANCH%"), "BRANCH")
        .otherwise(col("PreferredChannel"))
    )
)

# ------------------------------------------------
# 6. Result
# ------------------------------------------------
final_df.show(truncate=False)
final_df.printSchema()

final_df.write.mode("overwrite").option("header", "true").csv("gs://hack-hcl-bank-retail/source_data/customers_cure.csv")
print("Cleansed data write to bucket")

# ------------------------------------------------
# 7. (Optional) Save cleansed data
# ------------------------------------------------
# final_df.write.mode("overwrite").parquet("/tmp/cleansed/customers")
