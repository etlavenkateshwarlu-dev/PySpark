from pyspark.sql import SparkSession
# Sample employee dictionary data (Python)
from pyspark.sql.functions import col
from pyspark.sql.functions import count
from  pyspark.sql.functions import desc

spark=SparkSession.builder.appName("RDDToDataFrame").getOrCreate()
emp_df=spark.read.option('header',True).csv('employee_1.csv')


#using spark sql
emp_df.createOrReplaceTempView("emp_view")
order_by_sql=spark.sql("""
select * from emp_view order by eid desc
""")
print("=========spark SQL ==========")
order_by_sql.show()

#using Dataframe
print("=========spark Dataframe ==========")
order_by_df=emp_df.orderBy(desc("eid"))

order_by_df.show()