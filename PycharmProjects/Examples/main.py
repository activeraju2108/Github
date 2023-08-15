from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("my App").master("local[*]").getOrCreate()

df = spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","C:/Users/activ/Downloads/duplicate.csv")\
    .load()

#Approach 1
df1 = df.groupby("Name","Age","Education","Year").count()

df2 = df1.select("Name","Age","Education","Year").filter(col("count") > 1)

df2.show()

#Approach 2

win = Window.partitionBy("Name").orderBy(col("Year").desc())

df3 = df.withColumn("Row",row_number().over(win))

df3.filter(col("Row") > 1).drop("Row").dropDuplicates().show()