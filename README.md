from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
conf = SparkConf().setAppName("SocketKafkaForwardConsumer").setMaster("local[2]")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 10) # 2-second batch interval
lines = ssc.socketTextStream("localhost", 9999)
def process(rdd):
    count = rdd.count()
    if count > 0:
        print("Received {0} records in this batch".format(count))
        for i, record in enumerate(rdd.take(10), start=1):
            print("[{0}] {1}".format(i,record))
    else:
        print("No records in this batch")
lines.foreachRDD(process)
ssc.start()
ssc.awaitTermination()
