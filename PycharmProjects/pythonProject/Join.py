from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp,monotonically_increasing_id
from pyspark.sql.functions import *
from pyspark.sql.types import DateType

spark = SparkSession.builder.appName("My App").master("local[*]").getOrCreate()

dataList = [(1,"2023-01-01",11599,"CLOSED"),
            (2,"2023-08-08",256,"PENDING_PAYMENT"),
            (3,"2023-01-01",11599,"CLOSED"),
            (4,"2018-08-07",8827,"CLOSED"),
            (5,"2015-08-21",11318,"COMPLETE"),
            (6,"2015-09-14",7130,"COMPLETE")]

st = spark.createDataFrame(dataList).toDF("order_id","ts","cust_id","status")


st1 = st.withColumn("ts",st["ts"].cast(DateType()))

df = st1.withColumn("ts",unix_timestamp("ts"))
df1 = df.withColumn("new_id", monotonically_increasing_id())
df2 = df1.dropDuplicates(["status","cust_id"])
df3 = df2.drop("order_id").show()