from pyspark.sql import SparkSession

spark=SparkSession.builder.getOrCreate()
data = [
    {"id": 1, "name": "Amit", "age": 28},
    {"id": 2, "name": "Neha", "age": 25},
    {"id": 3, "name": "Rahul", "age": 30}
]

df=spark.createDataFrame(data)

df.show()