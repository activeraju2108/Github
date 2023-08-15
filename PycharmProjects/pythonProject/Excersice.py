from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("My App").master("local[*]").getOrCreate()

List = [('rajapandi@gmail.com','987678765'),('priyadharshini@gmail.com','9876543210')]

def email(x):
    f = x.split("@")[0]
    l = len(f)
    L = list(f)
    for i in range(1,l-1):
        L[i] = '*'
    s = ''.join(L)
    return s+'@'+x.split("@")[1]

def phone(y):
    t = len(y)
    b = list(y)
    for i in range(1,t-1):
        b[i] = '*'
    v = ''.join(b)
    return v


df = spark.createDataFrame(List,['email','contact'])

emailM = udf(email,StringType())
phoneM = udf(phone,StringType())

df1 = df.withColumn("Masked_Email",emailM(col("email"))).withColumn("Masked_Contact",phoneM(col("contact")))

df.show(truncate=False)
df1.show()