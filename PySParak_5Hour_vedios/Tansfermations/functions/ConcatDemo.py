from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import  upper
from pyspark.sql.functions import  lower
from pyspark.sql.functions import concat
from pyspark.sql.functions import concat_ws
spark=SparkSession.builder.appName("FunctionsDemo").getOrCreate()
emp_df=spark.read.option('header',True).option('inferSchema',True).csv('employee_1.csv')
emp_df.createOrReplaceTempView('emp_view')
print("================SQL==============")
#select_df=spark.sql("select  concat(fname,laname from emp_view")

colums_rename_map={"fname":"first_name",
                   "lname":"lastname",
                   "hire_date":"joining_date"
                   }
rename_df=emp_df
for key,value in colums_rename_map.items():
    rename_df=rename_df.withColumnRenamed(key,value)

final_df=rename_df.withColumn('full_name',concat_ws(' ',col('first_name'),col('lastname')))
final_df.show()
# select_df=emp_df.withColumn('full_name',concat(col('fname'),col('lname'))).select('eid','full_name')
# select_df.
