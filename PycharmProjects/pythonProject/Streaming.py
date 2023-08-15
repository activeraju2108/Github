from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

spark = SparkSession.builder.appName("MyApp").master("local[*]").getOrCreate()

sch = StructType[
    (
        StructField("order_id",IntegerType()),
        StructField("order_date",TimestampType()),
        StructField("custId",IntegerType()),
        StructField("order_status",StringType())
    )]


readDF = spark.readStream\
            .format("socket")\
            .option("host","localhost")\
            .schema(sch)\
            .option("port",9992)\
            .load()

readDF.printSchema()

