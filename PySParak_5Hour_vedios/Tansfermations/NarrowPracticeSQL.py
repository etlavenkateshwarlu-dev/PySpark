from pyspark.sql import SparkSession
# Sample employee dictionary data (Python)
from pyspark.sql.functions import col

spark=SparkSession.builder.appName("RDDToDataFrame").getOrCreate()
emp_df=spark.read.option('header',True).csv('employee_1.csv')
emp_df.createTempView('employee_view')
emp_view_df=spark.sql(
    """
    select eid,fname,country from employee_view where country='US'  limit 5
    """
)
emp_view_df.printSchema()
emp_view_df.show()
