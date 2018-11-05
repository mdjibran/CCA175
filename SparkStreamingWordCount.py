'''
spark-submit --master local[2] --conf "spark.dynamicAllocation.enabled=false" streamingWordCount.py localhost 9999
'''

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

conf = SparkConf(). \
setAppName("Streaming Department Count"). \
setMaster("yarn-client")
sc = SparkContext(conf=conf)

ssc = StreamingContext(sc, 15)

lines = ssc.socketTextStream("localhost", 9999)
words = lines.flatMap(lambda x: x.split(' '))
wrd = words.map(lambda x: (str(x), 1))
count = wrd.reduceByKey(lambda x, y: x+y)


count.print()
ssc.start()
ssc.awaitTermination()
