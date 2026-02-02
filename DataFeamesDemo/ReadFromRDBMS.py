from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Read data from RDBMS").config("spark.jars",r"C:\Users\91991\AppData\Roaming\DBeaverData\drivers\maven\maven-central\org.postgresql\postgresql-42.7.2.jar").getOrCreate()

jdbc_url="jdbc:postgresql://localhost:5432/postgres"
connection_properties={
    "user":"postgres",
    "password":"12345",
    "driver":"org.postgresql.Driver"
}
emp_df=spark.read.jdbc(jdbc_url,table="EMPY",properties=connection_properties)
emp_df.show()

