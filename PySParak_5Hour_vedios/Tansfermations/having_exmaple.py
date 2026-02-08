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
select eid,country ,count(*) as count_res from emp_view where country='IN' group by eid,country having count_res>1 
""")
print("=========spark SQL ==========")
order_by_sql.show()

#using Dataframe
print("=========spark Dataframe ==========")
df=emp_df.select("eid","country").where("country='IN'").groupBy("eid","country").agg(count("*").alias("total_count")).where("total_count >1")
#withColumn("count_res",agg(count("*)").groupBy("eid","country").

df.show()