from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("my App").master("local[*]").getOrCreate()

rdd = [(1,'Raja',21,'Pandi'),(2,'Hari',23,'Krishnan')]

df = spark.createDataFrame(rdd,["No","Name","Age","Name"])
#df.show()

a = df.columns
print(a)
dd = []
for x in a:
    if a.count(x) == 2:
        val = a.index(x)
        dd.append(val)

st = set(dd)

for x in st:
    a[x] = a[x] + '_0'

print(a)

df1 = df.toDF(*a)
df1.show()