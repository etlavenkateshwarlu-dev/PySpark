from pyspark.sql import SparkSession

spark=SparkSession.builder.getOrCreate()
rdd=spark.sparkContext.parallelize([(1,'ramesh',25),(2,'harish',25),(3,'sandesh',25)])
df=rdd.toDF(["id","name","age"])
df.show()

