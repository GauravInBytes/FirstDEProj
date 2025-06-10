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

data1 = [(1, "raj"), (2, "ravi"), (3, "sai"), (5, "rani")]
customer = spark.createDataFrame(data1, ["id", "name"])
data2 = [(1, "mouse"), (3, "mobile"), (7, "laptop")]
product = spark.createDataFrame(data2, ["id", "product"])

print("INNER JOIN")
innerJoin = customer.join(product, ["id"], "inner").orderBy("id")
innerJoin.show()

print("LEFT JOIN")
leftJoin = customer.join(product, ["id"], "left").orderBy("id")
leftJoin.show()

print("RIGHT JOIN")
rightoin = customer.join(product, ["id"], "right").orderBy("id")
rightoin.show()

print("FULL JOIN")
fullJoin = customer.join(product, ["id"], "full").orderBy("id")
fullJoin.show()

#################################################################################
#################################################################################
print("INNER JOIN Scenario")
data1 = [(1, "raj"), (2, "ravi"), (3, "sai"), (5, "rani")]
customer = spark.createDataFrame(data1, ["cid", "name"])
data2 = [(1, "mouse"), (3, "mobile"), (7, "laptop")]
product = spark.createDataFrame(data2, ["pid", "product"])

innerJoinResult = customer.join(product, customer["cid"] == product["pid"], "inner").orderBy("cid")
innerJoinResult.show()

#################################################################################
#################################################################################
data1 = [(1, "raj"), (2, "ravi"), (3, "sai"), (5, "rani")]
customer = spark.createDataFrame(data1, ["id", "name"])
data2 = [(1, "mouse"), (3, "mobile"), (7, "laptop")]
product = spark.createDataFrame(data2, ["id", "product"])

idlist  = product.select("id").rdd.flatMap(lambda x : x).collect()
print(idlist)

fildf = customer.filter( ~customer.id.isin(idlist))
fildf.show()

print("LEFT ANTI JOIN")
joindf = customer.join(product, ["id"] , "leftanti").orderBy("id")
joindf.show()

# print("CROSS JOIN")
# cross = customer.crossJoin(product)
# cross.show()

############# SECOND HIGHEST SALARY ---- V V V V V V V V V IMPORTANT ############
#################################################################################
print("SECOND HIGHEST SALARY ---- V V V V V V V V V IMPORTANT")
data = [("DEPT1", 1000), ("DEPT1", 700), ("DEPT1", 500), ("DEPT2", 400), ("DEPT2", 200), ("DEPT3", 500), ("DEPT3", 200)]
columns = ["department", "salary"]
df = spark.createDataFrame(data, columns)
df.show()

windowcreate = Window.partitionBy("department").orderBy(col("salary").desc())
denserankdf =   df.withColumn("rnk" , dense_rank().over(Window.partitionBy("department").orderBy(col("salary").desc())))
denserankdf.show()

result = denserankdf.filter("rnk = 2")
result.show()

result = result.drop("rnk")
result.show()

################# Scenario 1 ####################################################
#################################################################################
print("Scenario 1")
data1 = [(1, "Henry"), (2, "Smith"), (3, "Hall")]
columns1 = ["id", "name"]
df1 = spark.createDataFrame(data1, columns1)

data2 = [(1, 100), (2, 500), (4, 1000)]
columns2 = ["id", "salary"]
df2 = spark.createDataFrame(data2, columns2)

df1.show()
df2.show()

leftjoin = df1.join(df2, ["id"], "left").orderBy("id")
leftjoin.show()

result = leftjoin.withColumn("salary",expr("coalesce(salary,0)"))
result.show()

################# Scenario 2 ####################################################
#################################################################################
print("Scenario 2 => Getting Domain name from Email")
data = [(1, "Henry", "henry12@gmail.com"), (2, "Smith", "smith@yahoo.com"), (3, "Martin", "martin221@hotmail.com")]
columns = ["id", "name", "email"]
df = spark.createDataFrame(data, columns)
df.show()

result = df.withColumn("domain",expr("split(email,'@')[1]")).drop("email")
result.show()

################# Scenario 3 ####################################################
#################################################################################
print("Scenario 3")
data = [('2020-05-30','Headphone'),('2020-06-01','Pencil'),('2020-06-02','Mask'),('2020-05-30','Basketball'),('2020-06-01','Book'),('2020-06-02','Mask'),('2020-05-30','T-Shirt')]
columns = ["sell_date",'product']

df = spark.createDataFrame(data,schema=columns)
df.show()

result = df.groupBy("sell_date").agg(collect_set("product").alias("products"),size(collect_set("product")).alias("null_sell"))
result.show()
