from pyspark.sql import SparkSession
# Sample employee dictionary data (Python)
from pyspark.sql.functions import col
from pyspark.sql.functions import count

spark=SparkSession.builder.appName("RDDToDataFrame").getOrCreate()
emp_df=spark.read.option('header',True).csv('employee_1.csv')


#using spark sql
emp_df.createOrReplaceTempView("emp_view")
group_by_sql=spark.sql("""
select country,count(*) from emp_view group by country
""")
print("=========spark SQL ==========")
group_by_sql.show()

#using Dataframe
print("=========spark Dataframe ==========")
group_by_df=emp_df.groupBy("country").agg(count("*"))

group_by_df.show()