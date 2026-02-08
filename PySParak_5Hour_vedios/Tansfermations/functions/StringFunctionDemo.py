from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import  upper
from pyspark.sql.functions import  lower
spark=SparkSession.builder.appName("FunctionsDemo").getOrCreate()
emp_df=spark.read.option('header',True).option('inferSchema',True).csv('employee_1.csv')
emp_df.createOrReplaceTempView('emp_view')
print("================SQL==============")
select_df=spark.sql("select  upper(fname) ,upper(lname) from emp_view")
select_df.show()


print("================PySPark==============")
upper_df=emp_df.withColumn('first_name',lower(col('fname'))).select('fname','first_name')
upper_df.show()


