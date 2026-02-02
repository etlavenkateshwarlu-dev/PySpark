from pyspark.sql import SparkSession
from pyspark.sql.types import StructField
from pyspark.sql.types import  StructType
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType
from pyspark.sql.types import  DateType


#emp_id|first_name|last_name|department|job_title|location|gender|hire_date|salary|manager_id

input_schema=StructType([
    StructField('emp_id',StringType(),True),
    StructField('first_name',StringType(),True),
    StructField('last_name',StringType(),True),
    StructField('department',StringType(),True),
    StructField('job_title',StringType(),True),
    StructField('location',StringType(),True),
    StructField('gender',StringType(),True),
    StructField('hire_date',DateType(),True),
    StructField('salary',FloatType(),True),
    StructField('manager_id',StringType(),True)
])

spark=SparkSession.builder.appName('StructType').getOrCreate()

df=spark.read.option('header',True)\
    .option('delimiter','|')\
    .schema(input_schema)\
    .csv('gs://vctbatch-45-dataset/employee_pipe.csv')

   # .csv('gs://vctbatch-45-dataset/employee_1.csv')
    #.csv('employee_pipe.csv')

df.show()
df.printSchema()