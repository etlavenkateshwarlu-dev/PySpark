from pyspark.sql import SparkSession

#enableHiveSupport  to enable HIVE by default it will dissable in SparkSession
spark=SparkSession.builder.appName("").enableHiveSupport().getOrCreate()

df=spark.read.table("select * from db_name.emp_table")
df.show()

#hiev files stored in hdfs