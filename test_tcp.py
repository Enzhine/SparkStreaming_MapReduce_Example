from pyspark import SparkContext, RDD
from pyspark.streaming import StreamingContext
import os

os.environ['PYSPARK_PYTHON'] = 'python'

sc = SparkContext("local[2]", "SocketStreamExample")
ssc = StreamingContext(sc, batchDuration=5)

d_stream = ssc.socketTextStream("localhost", 9999)

word_stream = d_stream.flatMap(lambda line: line.split(' '))


def handle(rdd: RDD):
    word = rdd.collect()
    print(word)


word_counts = word_stream.foreachRDD(handle)

ssc.start()
ssc.awaitTermination(120)
