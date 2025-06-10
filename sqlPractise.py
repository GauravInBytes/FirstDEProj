# FULL SQL PRE REQUISITE

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

######################🔴🔴🔴################################

data = [
    (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
    (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
    (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
    (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
    (7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (8, "02-14-2011", 200.0, "Gymnastics", None, "cash")
]

df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df.show()

data2 = [
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "02-14-2011", 200.0, "Winter", None, "cash"),
    (7, "02-14-2011", 200.0, "Winter", None, "cash")
]

df1 = spark.createDataFrame(data2, ["id", "tdate", "amount", "category", "product", "spendby"])
# df1.show()

data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]

cust = spark.createDataFrame(data4, ["id", "name"])
# cust.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

prod = spark.createDataFrame(data3, ["id", "product"])
# prod.show()

# Register DataFrames as temporary views
df.createOrReplaceTempView("df")
df1.createOrReplaceTempView("df1")
cust.createOrReplaceTempView("cust")
prod.createOrReplaceTempView("prod")

# spark.sql("select * from df where category = 'Exercise'").show()
# spark.sql("select * from df where category in ('Exercise', 'Gymnastics')").show()
# spark.sql("select * from df where product is null").show()
# spark.sql("select *, case when spendby = 'cash' then 1 else 0 end as status from df").show()
# spark.sql("select id, category, concat(id, ' - ', category) as conData from df").show()
# spark.sql("select id, category, concat_ws(' - ', id, category, product) as conData from df").show()
# spark.sql("select product, coalesce(product, 'N/A') as withoutNull from df").show() # coalsec will replace null values with N/A in product column
# spark.sql("select product, split(product, ' ')[0] as SplitProduct from df").show()

# SPARK SQL
# jsonDf = spark.read.format("json").load("file4.json")
# jsonDf.show()
# jsonDf.createOrReplaceTempView("jsonDfView")
# spark.sql("select * from jsonDfView where category = 'Games'").show()


# SPARK DSL(Domain Specific Language)
jsonDf = spark.read.format("json").load("file4.json")
filteredData = jsonDf.filter("category = 'Games'")
filteredData2 = jsonDf.filter(jsonDf.category == "Games")
filteredData.show()
