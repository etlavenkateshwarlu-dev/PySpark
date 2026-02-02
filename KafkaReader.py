from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("KafkaEmployeeStreaming") \
    .getOrCreate()
# 2) Define schema matching your Kafka JSON structure
employee_schema = StructType([
    StructField("eid", IntegerType(), True),
    StructField("fname", StringType(), True),
    StructField("lname", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("hire_date", StringType(), True),   # keep as String (we can convert later)
    StructField("job_id", StringType(), True),
    StructField("salary", DoubleType(), True),
    StructField("dept_id", IntegerType(), True),
    StructField("country", StringType(), True)
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "employee-topic") \
    .option("startingOffsets", "latest") \
    .load()

# 4) Convert Kafka binary value to String
value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")

parsed_df = value_df.select(
    from_json(col("json_value"), employee_schema).alias("emp")
).select(
    col("emp.eid").alias("eid"),
    col("emp.fname").alias("fname"),
    col("emp.lname").alias("lname"),
    col("emp.phone_number").alias("phone_number"),
    col("emp.hire_date").alias("hire_date"),
    col("emp.job_id").alias("job_id"),
    col("emp.salary").alias("salary"),
    col("emp.dept_id").alias("dept_id"),
    col("emp.country").alias("country")
)

# 6) Write output to console
query = parsed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()