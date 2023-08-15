from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

spark = SparkSession.builder.appName("myApp").master("local[*]").getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
sc1 = StreamingContext(sc,10)
lines = sc1.socketTextStream("localhost",9998)

rdd1 = lines.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1))
rdd2 = rdd1.reduceByKeyAndWindow(lambda x,y:(x+y),lambda x,y:(x-y),20,10)
rdd2.pprint()

sc1.start()
sc1.awaitTermination()