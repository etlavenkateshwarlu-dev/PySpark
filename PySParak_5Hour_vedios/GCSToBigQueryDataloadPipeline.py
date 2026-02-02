from pyspark.sql import SparkSession
from pyspark.sql.functions import col

BIG_QUERY_TABLE='vctbatch-45:employee_pubsub_ds.pyspark_employee'
spark=SparkSession.builder.appName('Demo').getOrCreate()
#df=spark.read.option('header',True).csv('employee_1.csv')
df=spark.read.option('header',True).csv('gs://vctbatch-45-dataset/employee_1.csv')

df.write.format('bigquery').option('table',BIG_QUERY_TABLE)\
    .option("temporaryGcsBucket", "vctbatch-45-bq-temp") \
    .option("createDisposition", "CREATE_IF_NEEDED")\
    .option("writeDisposition", "WRITE_APPEND") \
    .mode('append').save()

print('Save to Big Query ')
df.show()
