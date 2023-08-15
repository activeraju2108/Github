from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

spark = SparkSession.builder.appName("myApp").master("local[*]").getOrCreate()

sc = spark.sparkContext
sc1 = StreamingContext(sc,5)
lines = sc1.socketTextStream("localhost",9998)
sc1.checkpoint("E:/checkpoint")

def newfunc(value,prev):
    if prev is None:
        prev = 0
    return sum(value,prev)

td = lines.flatMap(lambda x:x.split(" "))
rdd2 = td.map(lambda x:(x,1))
rdd3 = rdd2.updateStateByKey(newfunc)
rdd3.pprint()
sc1.start()
sc1.awaitTermination()
