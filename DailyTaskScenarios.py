import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


data_dir = "data"
os.makedirs(data_dir, exist_ok=True)
data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] = "hadoop"
os.environ['JAVA_HOME'] = r'C:\Users\gode4\.jdks\corretto-1.8.0_452'

spark = SparkSession.builder.appName("SQL Practice").master("local[*]").getOrCreate()

######### Scenario 1 ################################################
# print("Scenario 2 => Win count of each team")
# data = [
#     ('A', 'D', 'D'),
#     ('B', 'A', 'A'),
#     ('A', 'D', 'A')
# ]
#
# df = spark.createDataFrame(data).toDF("TeamA", "TeamB", "Won")
# df.show()
# # allTeams = df.select(explode(array("TeamA", "TeamB")).alias("TeamName")).distinct()
# allTeams = df.select("TeamA").union(df.select("TeamB")).distinct().toDF("TeamName")
# allTeams.show()
#
# result = allTeams.join(
#     df.groupBy("Won").agg(count("Won").alias("wonCount")),
#     allTeams["TeamName"] == df["Won"],
#     "left"
# ).fillna(0).drop("Won").withColumnRenamed("wonCount", "Won")
# result.show()

######### Scenario 2 ################################################
print("Scenario 2 => Using Pivot")
data = [
    (101, "Eng", 90),
    (101, "Sci", 80),
    (101, "Mat", 95),
    (102, "Eng", 75),
    (102, "Sci", 85),
    (102, "Mat", 90)
]
columns = ["Id", "Subject", "Marks"]
rdd = spark.sparkContext.parallelize(data)
df = rdd.toDF(columns)
df.show()
# It’s a safe way to fetch the mark since each (Id, Subject) pair appears only once.
# If there were duplicates, first() would just take the first one.
result = df.groupBy("Id").pivot("Subject").agg(first("Marks"))
result.show()



