import sys
import os
import time
from pyspark import SparkContext

print("spark start")
start = time.time()

logFile = "/usr/local/share/datasets/wikitext/enwiki-20170501-pages-articles1.xml-p10p30302"
sc = SparkContext()
#logData = sc.textFile(logFile).cache()
logData = sc.textFile(logFile)

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

elapsed_time = time.time() - start

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
print(("elapsed_time:{0}".format(elapsed_time)) + "[sec]")

sc.stop()

start = time.time()
print("local start")

sc = SparkContext("local")
#logData = sc.textFile(logFile).cache()
logData = sc.textFile(logFile)

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

elapsed_time = time.time() - start

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
print(("elapsed_time:{0}".format(elapsed_time)) + "[sec]")

sc.stop()