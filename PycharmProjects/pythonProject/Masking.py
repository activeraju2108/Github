from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder\
            .appName("Masking")\
            .master("local[*]")\
            .getOrCreate()

def mobile_mask(a):
    n = len(str(a))
    b = list(str(a))
    b[2:n-2]='*'* (n-4)
    fin = ''.join(b)
    return fin

def email_mask(b):
    content = b.split("@")[0]
    n = len(content)
    c = list(content)
    c[1:n-1]='*'* (n-2)
    fin = ''.join(c)+'@'+b.split("@")[1]
    return fin


mask_udf = udf(mobile_mask,StringType())
email_udf = udf(email_mask,StringType())

df = spark.read.format("csv") \
    .option("path", "E:/data.txt") \
    .option("header", True) \
    .option("inferSchema",True) \
    .load()

df.printSchema()
df.show()

df1 = df.withColumn("mobile_mask", mask_udf(col("Contact")))
df2 = df1.withColumn("email_mask", email_udf(col("Mail")))
dropped = ['Mail','Contact']
df3 = df2.drop(*dropped)
df3.show(truncate=False)
