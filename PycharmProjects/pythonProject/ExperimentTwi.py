from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Experiment Two").enableHiveSupport().master("local[*]").getOrCreate()
sc = spark.sparkContext
spark.conf.set("num-executors",10)
spark.conf.set("executor-cores",2)
spark.conf.set("executor-memory",'2G')

df = spark.read.format("csv").option("header",True).option("path","E:/dataset3.csv").load()

df1 = df.withColumn("RechargeDate",to_date(col("RechargeDate").cast("String"),'yyyyMMdd'))

df2 = df1.withColumn("New_date",expr("date_add(RechargeDate,Remaining_Days)"))

df2.show()
