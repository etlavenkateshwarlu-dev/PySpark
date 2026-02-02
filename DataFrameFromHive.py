from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
spark=SparkSession.builder.appName("ReadCSV").getOrCreate()
print(type(spark))

input_df=spark.read\
    .option("header",True)\
    .csv(r"C:\Users\91991\Documents\VT\Learnnings\Varahi_GCP_DataEng\Python\varahi_pythin_workspace"
         r"\PySprakPractice\DataFeamesDemo\data\customer_ny.csv")
#   print(input_df.show())

df=input_df.select("customer_id","cust_name","salesman_id").groupby("salesman_id").count()


#input_df.show()