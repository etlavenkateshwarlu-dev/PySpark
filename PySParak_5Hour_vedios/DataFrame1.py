from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark=SparkSession.builder.appName('Demo').getOrCreate()
df=spark.read.option('header',True).csv('employee_1.csv')
#df=spark.read.option('header',True).csv('gs://vctbatch-45-dataset/employee_1.csv')

df.write.parquet('employee_1.parquet')

pq_df=spark.read.parquet('employee_1.parquet')
pq_df.show()
