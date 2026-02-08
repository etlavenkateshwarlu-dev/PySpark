from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, initcap, to_date, when

spark = SparkSession.builder.appName("retail-cleansing").getOrCreate()

base_input  = "gs://hack-hcl-bank-retail/source_data"
base_output = "gs://hack-hcl-bank-retail/cleansing_data"

""" ---------------------------------------------------
# 1. Locations
Source Files :
gs://hack-hcl-bank-retail/source_data/Customers.csv
gs://hack-hcl-bank-retail/source_data/Locations.csv
gs://hack-hcl-bank-retail/source_data/Merchants.csv
gs://hack-hcl-bank-retail/source_data/Time.csv
gs://hack-hcl-bank-retail/source_data/Transactions.csv
--------------------------------------------------- """
# ------------------------------------------------
# 1. Read CSV
# ------------------------------------------------
customers_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(f"{base_input}/Customers.csv")
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

clean_customers_df = (
    customers_df
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
valid_customers_df = (
    clean_customers_df
    .filter(col("CustomerID").rlike("^C[0-9]+$"))
    .filter(col("Segment").isin("STUDENT", "PREMIUM", "STANDARD"))
    .filter(col("PreferredChannel").isin("DIGITAL", "ATM", "BRANCH"))
)
# ------------------------------------------------
# 4. Handle duplicates (business key = CustomerID)
# ------------------------------------------------
dedup_customers_df = valid_customers_df.dropDuplicates(["CustomerID"])
# ------------------------------------------------
# 5. Example of standardizing / fixing bad values
# (If future files contain variations like "Atm", "atm", etc.)
# ------------------------------------------------
final_customers_df = (
    dedup_customers_df
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
final_customers_df.show(truncate=False)
final_customers_df.printSchema()

final_customers_df.write.mode("overwrite").parquet(
    f"{base_output}/Customers_curated.parquet"
)

locations_df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{base_input}/Locations.csv")
)

locations_clean = (
    locations_df
        .select(
            trim(col("LocationID")).alias("LocationID"),
            initcap(trim(col("City"))).alias("City"),
            upper(trim(col("Country"))).alias("Country"),
            initcap(trim(col("Region"))).alias("Region")
        )
        .filter(col("LocationID").isNotNull())
        .dropDuplicates(["LocationID"])
)

locations_clean.write.mode("overwrite").parquet(
    f"{base_output}/Locations_curated.parquet"
)

# ---------------------------------------------------
# 2. Merchants
# ---------------------------------------------------
merchants_df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{base_input}/Merchants.csv")
)

merchants_clean = (
    merchants_df
        .select(
            trim(col("MerchantID")).alias("MerchantID"),
            initcap(trim(col("MerchantName"))).alias("MerchantName"),
            upper(trim(col("MerchantCategory"))).alias("MerchantCategory"),
            upper(trim(col("RiskLevel"))).alias("RiskLevel")
        )
        .filter(col("MerchantID").isNotNull())
        .dropDuplicates(["MerchantID"])
)

merchants_clean.write.mode("overwrite").parquet(
    f"{base_output}/Merchants_curated.parquet"
)

# ---------------------------------------------------
# 3. Time
# ---------------------------------------------------
time_df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{base_input}/Time.csv")
)

time_clean = (
    time_df
        .select(
            trim(col("TimeID")).alias("TimeID"),
            to_date(col("Date")).alias("Date"),
            upper(trim(col("DayOfWeek"))).alias("DayOfWeek"),
            col("Month").cast("int").alias("Month"),
            col("Year").cast("int").alias("Year"),
            col("HourOfDay").cast("int").alias("HourOfDay")
        )
        .filter(col("TimeID").isNotNull())
        .dropDuplicates(["TimeID"])
)

time_clean.write.mode("overwrite").parquet(
    f"{base_output}/Time_curated.parquet"
)

# ---------------------------------------------------
# 4. Transactions
# ---------------------------------------------------
transactions_df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{base_input}/Transactions.csv")
)

transactions_clean = (
    transactions_df
        .select(
            trim(col("TransactionID")).alias("TransactionID"),
            trim(col("CustomerID")).alias("CustomerID"),
            trim(col("MerchantID")).alias("MerchantID"),
            trim(col("TimeID")).alias("TimeID"),
            trim(col("LocationID")).alias("LocationID"),
            col("Amount").cast("double").alias("Amount"),
            upper(trim(col("Category"))).alias("Category"),
            upper(trim(col("Channel"))).alias("Channel")
        )
        .filter(col("TransactionID").isNotNull())
        .filter(col("Amount").isNotNull())
)

transactions_clean.write.mode("overwrite").parquet(
    f"{base_output}/Transactions_curated.parquet"
)

