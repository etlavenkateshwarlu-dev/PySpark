
from pyspark.sql import SparkSession
import os

#os.environ["PYSPARK_PYTHON"] = r"C:\Users\91991\anaconda3\python.exe"
#os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\91991\anaconda3\python.exe"

spark=SparkSession.builder.getOrCreate()

rdd=spark.sparkContext.parallelize(range(10),15)
print("rdd.collect()-->",rdd.collect())
print("rdd.glom().collect()-->",rdd.glom().collect())
rdd1=rdd.coalesce(15)
rdd2=rdd.repartition(2)
print("rdd1.collect() --> coalesce-->",rdd1.glom().collect())
print("rdd2.collect()-->",rdd2.glom().collect())




#
# input_df=spark.read.option("header",True)\
#     .csv(r"C:\Users\91991\Documents\VT\Learnnings\Varahi_GCP_DataEng\Python\varahi_pythin_workspace"
#          r"\PySprakPractice\DataFeamesDemo\data\customer_ny.csv")

#input_df.show()
#emp_view_df=input_df.createOrReplaceTempView("emp_view")
#df=spark.sql("select * from emp_view where customer_id='3002'")
#df.show()