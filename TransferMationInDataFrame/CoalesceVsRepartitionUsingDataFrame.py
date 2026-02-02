from pyspark.sql import  SparkSession
from pyspark.sql.functions import  spark_partition_id
spark=SparkSession.builder.getOrCreate()
input_df=spark.read.option("header",True)\
    .csv(r"C:\Users\91991\Documents\VT\Learnnings\Varahi_GCP_DataEng\Python\varahi_pythin_workspace"
         r"\PySprakPractice\DataFeamesDemo\data\customer_ny.csv")
df=input_df.repartition(3).withColumn("partition_id",spark_partition_id())

df.show()
#print(df.dtypes)

#print(df.count())
#df.explain('codegen')
