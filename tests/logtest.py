import sys
import os
from pyspark import SparkContext
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../tensorFlow_test/lib/utils")
from conf import Conf
from log import Log
import ylogger

old_log = Log.getLogger()
ylogger = ylogger.YarnLogger()

old_log.info("logtest start")

def myfunc(number):
	print(str(number))
	#old_log.debug(number)
	#ylogger.info("x: " + str(x))

if __name__ == "__main__":
	"""
	list = [1,2,3]
	for num in list:
		myfunc(num)
	"""
	sc = SparkContext.getOrCreate()
	ylogger_file = os.path.dirname(os.path.abspath(__file__)) + "/../../tensorFlow_test/lib/utils/ylogger.py"
	sc.addPyFile(ylogger_file)
	params = sc.parallelize(range(1000))
	old_log.info("reduce start")
	ylogger.info("this is ylogger")
	params.reduceByKey(lambda x: myfunc(x))
	sc.stop()
	print("Finished!!")