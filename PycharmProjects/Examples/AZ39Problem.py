from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("my App").master("local[*]").getOrCreate()

sc=spark.sparkContext

text = sc.textFile("E:/input.txt")

header = text.filter(lambda x:x.startswith("Property_ID"))
rdd = text.filter(lambda x: not x.startswith("Property_ID"))

dd = header.first().split("|")
f1 = dd.index("Property_ID")
f2 = dd.index("Location")
f3 = dd.index("Size")
f4 = dd.index("Price_SQ_FT")

def fun(x,y):
    return str(float(x)*float(y))


header_out = header.map(lambda x:x.split("|")[0]+'|'+x.split("|")[1]+"|Final_Price")
rdd_out = rdd.map(lambda x:x.split("|")[0]+'|'+x.split("|")[1]+'|'+fun(x.split("|")[5],x.split("|")[6]))

df = header_out.union(rdd_out)
st = df.collect()

for x in st:
    print(x)