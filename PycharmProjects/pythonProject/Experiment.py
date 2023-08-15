from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit

spark = SparkSession.builder.appName("MyApp").master("local[*]").getOrCreate()
sc = spark.sparkContext

df1 = spark.read.option("delimiter","|").option("inferSchema",True).option("header",True).csv("E:/dataset1.csv")
df2 = spark.read.option("delimiter","|").option("inferSchema",True).option("header",True).csv("E:/dataset2.csv")

df1.show()
df2.show()

df = df1.withColumn("Gender",lit("null"))

finaldata = df.union(df2)
finaldata.show()