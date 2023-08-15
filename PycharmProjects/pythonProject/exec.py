from pyspark import SparkContext, SparkConf, StorageLevel
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

spark = SparkSession.builder\
        .appName("My App")\
        .master("local[*]")\
        .getOrCreate()


df = spark.read\
    .format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","E:/orders.csv")\
    .load()

print(df.rdd.getNumPartitions())

spark.repartition(4).write\
    .format("csv")\
    .option("path","E:\myfolder")\
    .Savemode("Overwrite")\
    .save()
