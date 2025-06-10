import os
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
'''
print("Scenario 1 => Getting equal salary")
data = [("001", "Monika", "Arora", 100000, "2014-02-20 09:00:00", "HR"),("002", "Niharika", "Verma", 300000, "2014-06-11 09:00:00", "Admin"),("003", "Vishal", "Singhal", 300000, "2014-02-20 09:00:00", "HR"),("004", "Amitabh", "Singh", 500000, "2014-02-20 09:00:00", "Admin"),("005", "Vivek", "Bhati", 500000, "2014-06-11 09:00:00", "Admin")]
myschema = ["workerid","firstname","lastname","salary","joiningdate","depart"]
df = spark.createDataFrame(data,schema=myschema)
df.show()

window_spec = Window.partitionBy("salary")
partitionedData = df.withColumn("count", count("*").over(window_spec))
result = partitionedData.filter("count > 1").drop("count")
result.show()
'''

######### Scenario 2 ################################################
'''
print("Scenario 2 => dates when the status gets changed like ordered to dispatched")
data = [
    (1, "1-Jan", "Ordered"),
    (1, "2-Jan", "dispatched"),
    (1, "3-Jan", "dispatched"),
    (1, "4-Jan", "Shipped"),
    (1, "5-Jan", "Shipped"),
    (1, "6-Jan", "Delivered"),
    (2, "1-Jan", "Ordered"),
    (2, "2-Jan", "dispatched"),
    (2, "3-Jan", "shipped")]
myschema = ["orderid","statusdate","status"]
df = spark.createDataFrame(data,schema=myschema)
df.show()
# Approach 1
orderIds = df.filter(col("status") == "Ordered").select("orderid").rdd.flatMap(lambda x: x).collect()
result = df.filter((col('orderid').isin(orderIds)) & (col('status') == 'dispatched'))
result.show()
# Approach 2
ordered_df = df.filter(col("status") == "Ordered").select("orderid")
result = df.filter(col("status") == "dispatched").join(ordered_df, on="orderid", how="inner")
result.show()
'''

######### Scenario 3 ################################################
'''
print("Scenario 3 => Timestamp Difference - ")
data = [(1111, "2021-01-15", 10),
        (1111, "2021-01-16", 15),
        (1111, "2021-01-17", 30),
        (1112, "2021-01-15", 10),
        (1112, "2021-01-15", 20),
        (1112, "2021-01-15", 30)]

df = spark.createDataFrame(data, ["sensorid", "timestamp", "values"])
df.show()

window_spec = Window.orderBy("sensorid")

df_with_lead = df.withColumn("values", lead("values", 1).over(window_spec) - df.values).filter(col("values").isNotNull() & (col("values") > 0))
df_with_lead.show()

df_with_lead = df.withColumn("next_value", lead("values", 1).over(window_spec))

# Calculate the difference
df_diff = df_with_lead.withColumn("values", col("next_value") - col("values")) \
    .drop("next_value")

# Remove rows where difference is null (i.e., last row per group)
result = df_diff.filter(col("values").isNotNull() & (col("values") > 0))

result.show() 
'''

######### Scenario 4 ################################################
'''
print("Scenario 4 => unique customer names, number of addresses - ")
data = [(1, "Mark Ray", "AB"),
        (2, "Peter Smith", "CD"),
        (1, "Mark Ray", "EF"),
        (2, "Peter Smith", "GH"),
        (2, "Peter Smith", "CD"),
        (3, "Kate", "IJ")]
myschema = ["custid", "custname", "address"]
df = spark.createDataFrame(data, schema=myschema)
df.show()

result = df.groupBy(["custid", "custname"]).agg(collect_set("address"))
result.show()
'''

######### Scenario 5 ################################################
'''
print("Scenario 5 => Data with valid emails from two tables and a new column that is constant - ")
data1 = [
        (1, "abc", 31, "abc@gmail.com"),
        (2, "def", 23, "defyahoo.com"),
        (3, "xyz", 26, "xyz@gmail.com"),
        (4, "qwe", 34, "qwegmail.com"),
        (5, "iop", 24, "iop@gmail.com"),
]

df1 = spark.createDataFrame(data1, ["id", "name", "age", "email"])
df1.show()

data2 = [
        (11, "jkl", 22, "abc@gmail.com", 1000),
        (12, "vbn", 33, "vbn@yahoo.com", 3000),
        (13, "wer", 27, "wer", 2000),
        (14, "zxc", 30, "zxc.com", 2000),
        (15, "lkj", 29, "lkj@outlook.com", 2000),
]

df2 = spark.createDataFrame(data2, ["id", "name", "age", "email", "salary"])
df2.show()

result = df1.withColumn("salary", lit(1000)).union(df2).filter("email like '%@%'")
result.show()
'''

######### Scenario 6 ################################################
print("Scenario 6 => Designate manager and emploee based on salary - ")
data = [
        ("1", "a", 10000),
        ("2", "b", 5000),
        ("3", "c", 15000),
        ("4", "d", 25000),
        ("5", "e", 50000),
        ("6", "f", 7000),
]

df = spark.createDataFrame(data, ["empid", "name", "salary"])
df.show()

result = df.withColumn("Designation", expr("case when salary > 10000 then 'Manager' else 'Employee' end"))
result.show()

df_with_designation = df.withColumn(
        "designation",
        when(df.salary > 10000, "Manager").otherwise("Employee")
)
df_with_designation.show()