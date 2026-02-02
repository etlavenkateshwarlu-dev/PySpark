from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('Test').getOrCreate()
rdd=spark.sparkContext
rd1=rdd.parallelize([1,2,3,4,5,6,7])
print(rd1.getNumPartitions())

new_rdd=rd1.repartition(3)

print(new_rdd.glom().collect())

print(new_rdd.getNumPartitions())

new_rdd2=rd1.coalesce(3)


print("=======coalesce===")
print(new_rdd2.glom().collect())

print(new_rdd2.getNumPartitions())



