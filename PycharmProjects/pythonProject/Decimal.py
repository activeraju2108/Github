from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("My App").master("local[*]").enableHiveSupport().getOrCreate()

data = [("Data1",45.834738),("Data2",76.32231),("Data3",67.2476274)]
df = spark.createDataFrame(data,['Name','Decimal'])
df1 = df.withColumn("Decimal",format_number(col("Decimal"),2))
df1.show()
df1.write.bucketBy(2,"Name").saveAsTable('MyHiveTB')