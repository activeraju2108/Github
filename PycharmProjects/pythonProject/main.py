from pyspark import SparkContext
import sys

sc = SparkContext("local[*]","word")
text = sc.textFile("E:/Trendy Tech/Week 9(Spark)/Spark_Notes.txt")
rdd1 = text.flatMap(lambda x:x.split(" "))
rdd1.collect



