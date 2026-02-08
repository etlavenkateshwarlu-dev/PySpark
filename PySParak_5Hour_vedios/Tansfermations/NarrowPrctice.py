from pyspark.sql import SparkSession
# Sample employee dictionary data (Python)
from pyspark.sql.functions import col

spark=SparkSession.builder.appName("RDDToDataFrame").getOrCreate()
emp_df=spark.read.option('header',True).csv('employee_1.csv')

#select_df=emp_df.select('eid','fname','country').where("country = 'IN' ")
group_by_df=emp_df.select("country").groupBy("country").count("*")
#group_by_df=emp_df.groupBy("country").count()

group_by_df.show()