from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("ReadFromPQ").getOrCreate()

pq_df=spark.read.parquet(r"C:\Users\91991\Documents\VT\Learnnings\Varahi_GCP_DataEng\Python\varahi_pythin_workspace"
         r"\PySprakPractice\DataFeamesDemo\data\customer_ny_new.parquet1")
print("==================")

pq_df.show()

pq_df.write.mode("overwrite").saveAsTable("cust_table")

spark1=SparkSession.builder.appName().enableHiveSupport().getOrCreate()

df1=spark1.read.table("cust_table")
df1.show()