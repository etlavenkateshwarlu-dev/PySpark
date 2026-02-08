from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('PGSQL_DB_Connect')\
    .config("spark.jars",r"C:\Users\91991\Downloads\postgresql-42.7.3.jar")\
    .getOrCreate()

# JDBC URL
jdbc_url = "jdbc:postgresql://localhost:5432/postgres"

# Connection properties
pg_properties = {
    "user": "postgres",
    "password": "12345",
    "driver": "org.postgresql.Driver",
    # optional but useful
    "fetchsize": "10000"
}
table_name = "dwh.customer"
df=spark.read.jdbc(url=jdbc_url,properties=pg_properties,table=table_name)
df.show()