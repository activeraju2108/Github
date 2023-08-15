from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def updateFunct(newValue,previousState):
    if previousState is None:
        previousState = 0
    return sum(newValue,previousState)

sc = SparkContext("local[*]","StreamingOne")

ssc = StreamingContext(sc,10)

lines = ssc.socketTextStream("localhost",9998)

rdd1 = lines.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1))
rdd2 = rdd1.updateStateByKey(updateFunct)

rdd2.pprint()
ssc.awaitTermination()