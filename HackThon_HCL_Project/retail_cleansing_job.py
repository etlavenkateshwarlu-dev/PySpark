from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, initcap, to_date

spark = SparkSession.builder.appName("retail-cleansing").getOrCreate()

base_input  = "gs://hack-hcl-bank-retail/source_data"
base_output = "gs://hack-hcl-bank-retail/clensing_data"

# ---------------------------------------------------
# 1. Locations
# ---------------------------------------------------
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
    f"{base_output}/locations"
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
    f"{base_output}/merchants"
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
    f"{base_output}/time"
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
    f"{base_output}/transactions"
)

