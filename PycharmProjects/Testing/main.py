from pyspark.

spark = SparkSession.builder.appName("MyApp").master("local[*]").getOrCreate()

