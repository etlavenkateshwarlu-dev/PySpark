from pyspark.sql import SparkSession
# Sample employee dictionary data (Python)

employees = [
    {
        "emp_id": 101,
        "emp_name": "Ravi Kumar",
        "department": "IT",
        "designation": "Data Engineer",
        "salary": 85000,
        "city": "Hyderabad"
    },
    {
        "emp_id": 102,
        "emp_name": "Anita Sharma",
        "department": "HR",
        "designation": "HR Executive",
        "salary": 55000,
        "city": "Bengaluru"
    },
    {
        "emp_id": 103,
        "emp_name": "Suresh Reddy",
        "department": "Finance",
        "designation": "Accountant",
        "salary": 60000,
        "city": "Chennai"
    },
    {
        "emp_id": 104,
        "emp_name": "Priya Verma",
        "department": "IT",
        "designation": "Backend Developer",
        "salary": 90000,
        "city": "Pune"
    },
    {
        "emp_id": 105,
        "emp_name": "Karthik Rao",
        "department": "Sales",
        "designation": "Sales Manager",
        "salary": 75000,
        "city": "Mumbai"
    }
]

spark=SparkSession.builder.appName("RDDToDataFrame").getOrCreate()
rdd=spark.sparkContext.parallelize([(1,'jhone',25),(2,'ram',20),(3,'Jagan',28),])
schema=['edi','ename','age']
print(rdd.getNumPartitions())
print(rdd.glom().collect())
df=rdd.toDF(schema)
df.show()
print("=====================")


df=spark.createDataFrame(employees)
df.rdd.glom().coalesce(3)
df.show()