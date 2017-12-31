import sys
from pprint import pprint

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc=SparkContext.getOrCreate()
ssc=StreamingContext(sc,2)

ssc.checkpoint('/home/selva/spark/log')

lines=ssc.socketTextStream(sys.argv[1],int(sys.argv[2]))

def countValues(newCount,lastCount):
    if lastCount is None:
        lastCount=0
    return sum(newCount,lastCount)


word=lines.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).updateStateByKey(countValues)

word.pprint()

ssc.start()
ssc.awaitTermination()