from pyspark.sql import SparkSession

GCS_BUCKET_FILE = 'gs://vctbatch-45-dataset/employee_1.csv'
spark = SparkSession.builder.appName('WordCount').getOrCreate()
df = spark.read.text(GCS_BUCKET_FILE)
df.show()
print('================')
sc = spark.sparkContext
lines = sc.textFile(GCS_BUCKET_FILE)
words = lines.flatMap(lambda line: line.split())
word_pairs = words.map(lambda w: (w, 1))
word_count = word_pairs.reduceByKey(lambda a, b: a + b)
word_count.collect()
