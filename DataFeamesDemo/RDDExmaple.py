from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDD-glom-vs-collect").getOrCreate()
sc = spark.sparkContext

# Create RDD with 10 elements and 3 partitions
rdd = sc.parallelize(range(1, 11), 3)
print(rdd)
print("Number of partitions:", rdd.getNumPartitions())

# collect(): brings all elements as a single list
print("\ncollect() output:")
print(rdd.collect())

# glom(): groups elements per partition
print("\nglom().collect() output:")
glom_result = rdd.glom().collect()

