from pyspark.sql import SparkSession
GCS_BUCKET_FILE = 'gs://vctbatch-45-dataset/employee_1.csv'
class GCSFileChecker:
    def __init__(self,spark):
        self.spark=spark
        self.hadoop_config=spark._jsc.hadoopConfiguration()

    def file_exists(self,GCS_BUCKET_FILE):
        try:
            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark._jvm.java.net.URI(GCS_BUCKET_FILE),
                self.hadoop_conf
            )
            path = self.spark._jvm.org.apache.hadoop.fs.Path(GCS_BUCKET_FILE)
            return fs.exists(path)
        except Exception as e:
            print(e)



spark=SparkSession.builder.appName('FileCheckerInGCS').getOrCreate()
checker=GCSFileChecker(spark)
if checker.file_exists(GCS_BUCKET_FILE):
    print("File exists")
else:
    print("File NOT exists")

