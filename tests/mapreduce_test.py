import sys
import os
import numpy as np
from pyspark import SparkContext

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../tensorFlow_test/lib/utils")
from conf import Conf
from log import Log
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../../tensorFlow_test/lib/db")
from wikitext_mysql_operator import Mysql_operator
from table_wikitext_nodes import Table_nodes
from table_wikitext_edges import Table_edges

log = Log.getLogger()

def calc_edge_between(start_id, end_id, vectors):

	if start_id == end_id:
		return True
	elif start_id > end_id: #already registerd
		return True

	m = "from[" + str(start_id) + "] to[" + str(end_id) + "]"
	print(m)
	#log.debug(m)

	#distance = float(np.linalg.norm(vectors[start_id] - vectors[end_id]))
	#edge = Table_edges(start=start_id, end=end_id, relevancy=distance)
	#edge.insert()
	return True

def myfunc(a, b, v):
	print("aaa")

if __name__ == "__main__":
	log.info("get_edges_by_subtractions.py start.")
	
	sc = SparkContext.getOrCreate()
	db = Mysql_operator()
	records = db.session.query(Table_nodes).all()

	ids = []
	doc2vec = {}

	for record in records:
		ids.append(record.id)
		doc2vec[record.id] = np.array(record.doc2vec.split(","),dtype=float)

	#params = sc.parallelize(ids)
	#log.debug("params.count(): " + str(params.count()))
	#result = params.reduce(lambda s, e : calc_edge_between(s, e, doc2vec))
	#result = params.reduce(lambda s, e : s+e)
	#print(result)

	for start_id in [ids[0], ids[1]]:
		log.debug("start_id[" + str(start_id) + "] create param")
		params = sc.parallelize([{start_id: end_id} for end_id in ids])
		log.debug("params.count(): " + str(params.count()))
		#result = params.reduce(lambda s, e : myfunc(s, e, doc2vec))
		result = params.reduceByKey(lambda s, e : calc_edge_between(s, e, doc2vec))
		#result = params.reduceByKey(lambda s, e : print(s, e))
	log.debug("Finished!!")
	print("Finished!!")
