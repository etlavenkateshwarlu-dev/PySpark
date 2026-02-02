from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Test1").getOrCreate()
df=spark.read.option('header',True)\
    .option('delimiter','|')\
    .option('inferSchema',True)\
    .csv('employee_pipe.csv')

df.printSchema()
df.show()