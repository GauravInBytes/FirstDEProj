# AGG  CODE

import os
# from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
import sys

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] = "hadoop"
os.environ['JAVA_HOME'] = r'C:\Users\gode4\.jdks\corretto-1.8.0_452'

spark = SparkSession.builder \
    .appName("SQL Practice") \
    .master("local[*]") \
    .getOrCreate()

# data = [
#     ("sai", "10"),
#     ("zeyo", "20"),
#     ("sai", "10"),
#     ("zeyo", "10"),
#     ("sai", "20"),
#     ("zeyo", "10"),
#     ("sai", "10"),
#     ("zeyo", "10")
# ]
#
# df = spark.createDataFrame(data, ["name", "amt"])
# df.show()
#
from pyspark.sql.functions import *
#
# aggdf = (
#     df.groupBy("name")
#     .agg(
#         sum("amt").alias("total"),
#         count("amt").alias("cnt")
#     )
#     .withColumn("total", expr(" cast(total as int) "))
# )
#
# aggdf.show()
#
# aggdf.printSchema()
#
# # Scenarios
#
#
# data = [
#     (1, "Jhon", 4000),
#     (2, "Tim David", 12000),
#     (3, "Json Bhrendroff", 7000),
#     (4, "Jordon", 8000),
#     (5, "Green", 14000),
#     (6, "Brewis", 6000)
# ]
# df = spark.createDataFrame(data, ["emp_id", "emp_name", "salary"])
# df.show()
#
# procdf = df.withColumn("grade", expr("""
#                                         case
#                                         when salary > 10000 then 'A'
#                                         when salary between 5000 and 10000 then 'B'
#                                         else 'C'
#                                         end
#                                     """)
#                        )
# procdf.show()


data = [
    (1, 'Spark'),
    (1, 'Scala'),
    (1, 'Hive'),
    (2, 'Scala'),
    (3, 'Spark'),
    (3, 'Scala')
]
columns = ['id', 'subject']
df = spark.createDataFrame(data, columns)
df.show()

aggdf = df.groupBy("id").agg(
    collect_list("subject").alias("subject")
)

aggdf.show()


# SCENARIO 2
data = [(1, "Mark Ray", "AB"),
        (2, "Peter Smith", "CD"),
        (1, "Mark Ray", "EF"),
        (2, "Peter Smith", "GH"),
        (2, "Peter Smith", "CD"),
        (3, "Kate", "IJ")]
myschema = ["custid", "custname", "address"]
df = spark.createDataFrame(data, schema=myschema)
df.show()


aggdf = df.groupBy("custid","custname").agg(

    collect_set("address").alias("address")


).orderBy("custid")

aggdf.show()