from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

spark = SparkSession.builder.appName("myApp").master("local[*]").getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
sc1 = StreamingContext(sc,10)
lines = sc1.socketTextStream("localhost",9998)

def mainF(x,y):
    return str(int(x)+int(y))

def mainFu(x,y):
    return str(int(x)-int(y))

rdd1 = lines.reduceByWindow(mainF,mainFu,10,2)
rdd1.pprint()

sc1.start()
sc1.awaitTermination()