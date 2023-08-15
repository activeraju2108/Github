from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("My App").master("local[*]").getOrCreate()

df = spark.read.format("csv").option("inferSchema",True).option("header",True)\
            .option("path","E:/students.csv").load()

df1 = df.withColumn("subject",expr("replace(subject,' ','')"))


df1.write\
    .partitionBy("subject")\
    .mode("overwrite")\
    .format("parquet")\
    .option("path","E:/students")\
    .save()