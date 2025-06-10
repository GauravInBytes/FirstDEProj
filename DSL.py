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

data = [
    ("00000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00005", "02-14-2011", 200, "Gymnastics", None, "cash")
]

df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df.show()

# seldf = df.select( "tdate" , "product" )
# seldf.show()
#
#
# # dropdf = df.drop( "tdate" , "product" )
# # dropdf.show()
#
# sinfil  = df.filter("  category = 'Exercise' ")
# sinfil.show()
#
#
# mulfil = df.filter("  category ='Exercise'  and  spendby='cash'    ")
# mulfil.show()
#
#
# mulfilor =  df.filter("  category ='Exercise'  or  spendby='cash'    ")
# mulfilor.show()
#
# mulValueFil = df.filter("category in ('Exercise', 'Gymnastics')")
# mulValueFil.show()
#
# likefil = df.filter("   product like  '%Gymnastics%'    ")
# likefil.show()
#
# nulfill = df.filter(" product is null ")
# nulfill.show()
#
# notnullfill = df.filter(" product is not null ")
# notnullfill.show()
#
# notfill = df.filter(" category != 'Exercise' ")
# notfill.show()

procdf = df.selectExpr(
    "id",
    "split(tdate,'-')[2] as year",
    "amount+ 100 as amount",
    "upper(category) as category",
    "concat(product,'~zeyo') as product",
    "spendby",
    """
        case 
        when spendby='cash' then 0
        when spendby='credit' then 1 
        else 2
        end 
        as status  
        """

)
procdf.show()

from pyspark.sql.functions import *

procdf1 = (

    df.withColumn("category", expr(" upper(category) "))
    .withColumn("amount", expr("amount+100"))
    .withColumn("product", expr("concat(product,'~zeyo')"))
    .withColumn("tdate", expr("split(tdate,'-')[2]"))
    .withColumn("status", expr("case when spendby='cash' then 0 else 1 end"))

)

procdf1.show()

procdf2 = (

    df.withColumn("category", expr(" upper(category) "))
    .withColumn("amount", expr("amount+100"))
    .withColumn("product", expr("concat(product,'~zeyo')"))
    .withColumn("tdate", expr("split(tdate,'-')[2]"))
    .withColumn("status", expr("case when spendby='cash' then 0 else 1 end"))
    .withColumnRenamed("tdate", "year")
)

procdf2.show()
