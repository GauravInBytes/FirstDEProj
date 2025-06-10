import os
import urllib.request
import ssl

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt": os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe": os.path.join(data_dir1, "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll": os.path.join(data_dir1, "hadoop.dll")
}

# Create an unverified SSL context
ssl_context = ssl._create_unverified_context()

for url, path in urls_and_paths.items():
    # Use the unverified context with urlopen
    with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
        data = response.read()
        out_file.write(data)
import os, urllib.request, ssl

ssl_context = ssl._create_unverified_context()
[open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in
 {"https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv",
  "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv",
  "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt",
  "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt",
  "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt",
  "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt",
  "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json",
  "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet",
  "https://github.com/saiadityaus1/test1/raw/main/file6": "file6",
  "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv",
  "https://raw.githubusercontent.com/saiadityaus1/test1/refs/heads/main/state.txt": "state.txt",
  "https://github.com/saiadityaus1/SparkCore1/raw/refs/heads/master/data.orc": "data.orc",
  "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv",
  "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/refs/heads/master/rm.json": "rm.json"}.items()]

# ======================================================================================

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] = "hadoop"
os.environ['JAVA_HOME'] = r'C:\Users\gode4\.jdks\corretto-1.8.0_452'
######################🔴🔴🔴################################

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host", "localhost").set(
    "spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

# spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################print("==STARTED==")

print("==STARTED==")

#
# a = 2
# print(a)
#
# b = a + 2
# print(b)
#
# c = "zeyobron"
# print(c)
#
# print("==STARTED==")
#
# lis =  [ 1 , 2 , 3 , 4]
#
#
# rdd = sc.parallelize(lis)
# print(rdd.collect())
#
# add = rdd.map(lambda r : r + 2)
# print(add.collect())
#
# # 🔴FULL CODE
#
# print("==STARTED==")
#
# lis =  [ 1 , 2 , 3 , 4]
# print()
# print("======RAW LIST=========")
# print(lis)
#
# rdd = sc.parallelize(lis)
# print()
# print("======RDD=========")
# print(rdd.collect())
#
#
# addrdd = rdd.map( lambda x : x + 2)
# print()
# print("======addrdd=========")
# print(addrdd.collect())
#
#
#
# #[1, 2, 3, 4]
#
# mulrdd = rdd.map( lambda x  : x * 10 )
# print()
# print("======mulrdd=========")
# print(mulrdd.collect())
#
#
#
# #[1, 2, 3, 4]
#
# subrdd = rdd.map( lambda x : x  - 1)
# print()
# print("======subrdd=========")
# print(subrdd.collect())
#
#
#
#
#
# divrdd = rdd.map( lambda x : x / 10)
# print()
# print("======divrdd=========")
# print(divrdd.collect())
#
#
# print("==STARTED==")
#
# ls = [  "zeyobron"  ,  "zeyo"  ]
# print()
# print("=====RAWLIST======")
# print(ls)
#
#
# rdds  = sc.parallelize(ls)
# print()
# print("=====rdds======")
# print(rdds.collect())
#
# adrdd = rdds.map( lambda   x    :    x   +     "Analytics")
# print()
# print("=====adrdd======")
# print(adrdd.collect())
#
# print("=====filter rdd======")
# fil = rdd.filter(lambda x : x > 2)
# print(fil.collect())
#
# print("=====String map rdd======")
# stringList = ["Gode", "Is", "Great"]
# stringRDD = sc.parallelize(stringList)
# stringFilterRdd = stringRDD.map(lambda x : x + "s")
# stringFilterRdd2 = stringRDD.map(lambda x : 'e' in x) # true false result
# print(stringFilterRdd.collect())
# print(stringFilterRdd2.collect())
#
# print("=====String filter rdd======")
# stringList = ["Gode", "Is", "Great"]
# stringRDD = sc.parallelize(stringList)
# stringFilterRdd = stringRDD.filter(lambda x : 'e' in x)
# print(stringFilterRdd.collect())
#
#
# print("=====String replace rdd======")
# stringList = ["Godee", "heart", "Greeat"]
# stringRDD = sc.parallelize(stringList)
# stringFilterRdd = stringRDD.map(lambda x : x.replace('ee', 'gode'))
# print(stringFilterRdd.collect())
#
# print("=====String flat rdd======")
# data = ["A~B", "C~D"]
# rdd = sc.parallelize(data)
# mapped_rdd = rdd.map(lambda x: x.split("~")).collect() #map applies the function to each element but does NOT flatten the result.
# flattened_rdd = rdd.flatMap(lambda x: x.split("~")) #Applies a function to each element in the RDD, and flattens the result into a single list (not a list of lists).
# result = flattened_rdd.collect()
# print(mapped_rdd)
# print(result)

# StateList = ["State->UttarPradesh~City->Bareilly", "State->Telangana~City->Hyderabad"]
# StateListRdd = sc.parallelize(StateList)
#
# StateListRdd = StateListRdd.flatMap(lambda x : x.split("~"))
# states = StateListRdd.filter( lambda x : 'State' in x )
# cities = StateListRdd.filter(lambda x : 'City' in x )
# stateList = states.map(lambda x : x.replace("State->", ""))
# cityList = cities.map(lambda x : x.replace("City->", ""))
# print(StateListRdd.collect())
# print(stateList.collect())
# cityList.foreach(print)

################## =====  FILE READ ======== #########################
# print("File Read Operations")
# # The textFile() method will convert the data from text file into RDD directly
# data = sc.textFile("state.txt")
# print(data.collect())
# filteredData = data.flatMap(lambda x : x.split("~"))
# print(filteredData.collect())
# states = filteredData.filter( lambda x : 'State' in x ).map(lambda x : x.replace("State->", ""))
# cities = filteredData.filter(lambda x : 'City' in x ).map(lambda x : x.replace("City->", ""))
# # stateList = states.map(lambda x : x.replace("State->", ""))
# # cityList = cities.map(lambda x : x.replace("City->", ""))
# print(states.collect())
# cities.foreach(print)


################## =====  CSV FILE READ ======== #########################
# data = sc.textFile("usdata.csv")
# print(data.collect())
# filteredData = data.filter(lambda x : len(x) > 200)
# filteredData.foreach(print)
# flatData = filteredData.flatMap(lambda x : x.split(","))
# print(flatData.collect())
# replcaedData = flatData.map(lambda x : x.replace("-", ""))
# print(replcaedData.collect())
# concatenatedData = replcaedData.map(lambda x : x + ",Gode")
# print(concatenatedData.collect())
# # concatenatedData.saveAsTextFile("GodeData")
# # print("Data Written!!")

# Map columns from file and use a certain column and save as parquet file
data = sc.textFile("dt.txt")
splitData = data.map(lambda x: x.split(","))
from collections import namedtuple

columns = namedtuple('columns', ['id', 'tdate', 'amount', 'category', 'product', 'mode'])
schemaRdd = splitData.map(lambda x: columns(x[0], x[1], x[2], x[3], x[4], x[5]))
columnFilter = schemaRdd.filter(lambda x: 'Gymnastics' in x.product)
columnFilter.foreach(print)
df = columnFilter.toDF()
df.show()
# df.write.parquet("file:///C:\Users\gode4\OneDrive\Desktop\parqe")


csvdf = spark.read.format("csv").option("header", "true").load("usdata.csv")
csvdf.show()

jsondf = spark.read.format("json").load("file4.json")
jsondf.show()

parquetdf = spark.read.format("parquet").load("file5.parquet")
parquetdf.show()

# Add below line to open http://localhost:4040/jobs/ in Browser and to see jobs and DAG
# import time
# time.sleep(360)
