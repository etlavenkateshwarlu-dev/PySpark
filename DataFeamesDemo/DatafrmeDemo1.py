from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
spark=SparkSession.builder.appName("ReadCSV").getOrCreate()
print(type(spark))

input_file_schema=StructType([
    StructField("customer_id",IntegerType(),True),
    StructField("cust_name",StringType(),True),
    StructField("city",StringType(),True),
    StructField("grade",StringType(),True),
    StructField("salesman_id",IntegerType(),True)

])
input_df=spark.read\
    .option("header",False)\
    .option("inferschema",True)\
    .schema(input_file_schema)\
    .csv(r"C:\Users\91991\Documents\VT\Learnnings\Varahi_GCP_DataEng\Python\varahi_pythin_workspace"
         r"\PySprakPractice\DataFeamesDemo\data\customer_ny_noheader.csv")



# input_df=spark.read.option("header",True).option("delimeter","|")\
#      .csv(r"C:\Users\91991\Documents\VT\Learnnings\Varahi_GCP_DataEng\Python\varahi_pythin_workspace"
#           r"\PySprakPractice\DataFeamesDemo\data\customer_ny_pipe.csv")


#   print(input_df.show())
input_df.printSchema()
input_df.write.mode("overwrite").parquet(r"C:\Users\91991\Documents\VT\Learnnings\Varahi_GCP_DataEng\Python\varahi_pythin_workspace"
         r"\PySprakPractice\DataFeamesDemo\data\customer_ny_new.parquet1")

pq_df=spark.read.parquet(r"C:\Users\91991\Documents\VT\Learnnings\Varahi_GCP_DataEng\Python\varahi_pythin_workspace"
         r"\PySprakPractice\DataFeamesDemo\data\customer_ny_new.parquet1")
print("==================")
#pq_df.show()
#pq_df.printSchema()

pq_df.write.mode("overwrite").orc(r"C:\Users\91991\Documents\VT\Learnnings\Varahi_GCP_DataEng\Python\varahi_pythin_workspace"
         r"\PySprakPractice\DataFeamesDemo\data\customer_ny_orc.orc")


orc_df=spark.read.orc(r"C:\Users\91991\Documents\VT\Learnnings\Varahi_GCP_DataEng\Python\varahi_pythin_workspace"
         r"\PySprakPractice\DataFeamesDemo\data\customer_ny_orc.orc")

#orc_df.show()

orc_df.write.mode("overwrite").json(r"C:\Users\91991\Documents\VT\Learnnings\Varahi_GCP_DataEng\Python\varahi_pythin_workspace"
         r"\PySprakPractice\DataFeamesDemo\data\customer_ny_json.json")

#json_df=spark.read.json(r"C:\Users\91991\Documents\VT\Learnnings\Varahi_GCP_DataEng\Python\varahi_pythin_workspace"
 #        r"\PySprakPractice\DataFeamesDemo\data\customer_ny_json.json")

#json_df.select("city","cust_name").show()

#emp_json_df1=spark.read.option("multiline",True).json(r"C:\Users\91991\Documents\VT\Learnnings\Varahi_GCP_DataEng\Python\varahi_pythin_workspace\PySprakPractice\DataFeamesDemo\input_data\emp.json")

#emp_json_df1.printSchema()
#emp_json_df1.select("_corrupt_record").show(truncate=False)
#emp_final_df=emp_json_df1.select(explode(col("employees"))).alias("emp")
#emp_final_df.show(truncate=False)
#final_df=emp_final_df.select("emp.*").show()
#final_df.show(truncate=False)
#emp_json_df1.printSchema()

#
# emp_json_df1=spark.read.option("multiline",True)\
#     .json(r"C:\Users\91991\Documents\VT\Learnnings\Varahi_GCP_DataEng\Python"
#           r"\varahi_pythin_workspace\PySprakPractice\DataFeamesDemo\input_data\emp1.json")
#
#
# new_df=emp_json_df1.select("emp_id","emp_name","personal_info.*")
#
# new_df.show()