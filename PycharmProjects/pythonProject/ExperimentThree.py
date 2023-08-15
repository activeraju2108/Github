from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").master("local[*]").getOrCreate()

data = [[1001,'English',84],[1001,'Physics',55],[1001,'Maths',60],[1002,'English',69],[1002,'Physics',53],[1002,'Maths',87]]

order = ["English","Physics","Maths"]

df = spark.createDataFrame(data,['Rollno','Subject','Marks'])

pivot_df = df.groupBy("Rollno").pivot("Subject",order).sum("Marks")
pivot_df.show()