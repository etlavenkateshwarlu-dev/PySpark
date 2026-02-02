
from  pyspark.sql import SparkSession
spark=SparkSession.builder.appName("FileFormatConversion").getOrCreate();
df=spark.read.option("header","true").option("inferSchema","true").csv("customer_ny.csv")
df.write.mode("overwrite").parquet("customer_ny.parquet")
df1=spark.read.format("parquet").option("header","true").option("inferSchema","true").load("customer_ny.parquet")

df1.show