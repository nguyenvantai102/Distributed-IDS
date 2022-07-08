from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from functools import reduce

from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.shell import sqlContext
from pyspark.mllib.util import MLUtils
import time
from pyspark.mllib.util import MLUtils
from pyspark.sql import SparkSession, SQLContext

import json
import sys
import requests
import os
from pyspark.ml import PipelineModel as pipemodel

def load_data():
	spark = SparkSession \
	    .builder \
	    .appName("Spark Streaming") \
	    .getOrCreate()
	    
	# create schema 

	my_schema = StructType() \
	.add("8", StringType()) \
	.add("4", IntegerType()) \
	.add("35632.57590", DoubleType()) \
	.add("1", IntegerType()) \
	.add("23", IntegerType()) \
	.add("2", IntegerType()) \
	.add("24", IntegerType()) \
	.add("161", IntegerType()) \
	.add("6", IntegerType()) \
	.add("35632.57550", IntegerType()) \
	.add("35632.57551", IntegerType()) \
	.add("35632.57981", IntegerType()) \
	.add("35632.57863", IntegerType()) \
	.add("35632.57864", IntegerType()) \
	.add("35632.57579", IntegerType()) \
	.add("35632.57580", IntegerType()) \
	.add("25", IntegerType()) \
	.add("26", IntegerType()) \
	.add("35632.57944", DoubleType()) \
	.add("35632.57945", DoubleType()) \
	.add("35632.57599", IntegerType()) \
	.add("35632.57581", IntegerType()) \
	.add("35632.57600", IntegerType()) \
	.add("35632.57582", IntegerType()) \
	.add("35632.57556", LongType()) \
	.add("35632.57559", LongType()) \
	.add("35632.57560", IntegerType()) \
	.add("35632.57561", IntegerType()) \
	.add("35632.57562", IntegerType()) \
	.add("35632.57563", IntegerType()) \
	.add("35632.57564", IntegerType()) \
	.add("35632.57888", IntegerType()) \
	.add("35632.57892", IntegerType()) \
	.add("32", IntegerType()) \
	.add("176", IntegerType()) \
	.add("35632.57678", IntegerType()) \
	.add("35632.57679", IntegerType()) \
	.add("35632.57824", LongType()) \
	.add("35632.57831", DoubleType())

	df = spark \
	  .readStream \
	  .format("kafka") \
	  .option("kafka.bootstrap.servers", "192.168.119.131:9092") \
	  .option("subscribe", "topicFlows") \
	  .load() \
	  .selectExpr("CAST(value AS STRING)")

	netflow_df = df \
		.select(from_json(col("value"), my_schema) \
		.alias("netflow"))
		
	nf_data = netflow_df.select("netflow.*")
	# The JSON data is in the Kafka message value.
	# We need to read it as json (using from_json function) using our schema,
	# and then select the subfields into a top level DataFrame
	#.selectExpr("CAST(8 AS STRING) AS key", "to_json(struct(*)) AS value")

	return nf_data


def sendToONOS(anomalyIP):
	# First get a list of all hosts.
	# Create dictionary mapping from IP to host switch.
	ipToSwitchMap = {}
	ipToSwitchPortMap = {}
	
	hosts = requests.get('http://192.168.115.131:8181/onos/v1/hosts/', auth=('onos', 'rocks'))
	
	host_json = hosts.json()
	for host in host_json['hosts']:
		IP = host['ipAddresses'][0]
		switch = host['locations'][0]['elementId']
		port = host['locations'][0]['port']
		mac_add = host['mac']
		ipToSwitchMap[IP] = switch
	
	# Send a request to ONOS to drop traffic for that anomalyIP from the
	# switch the bad device is connected on.
	print("Send to ONOS: Need to block {0}".format(anomalyIP))
	# Configure parameters needed for POST request
	blockData = {
	"flows" [
		{
		"priority": 40000,
		"timeout": 0,
		"isPermanent": "true",
		"deviceId": ipToSwitchMap[anomalyIP],
		"treatment": {},  # blank treatment means drop traffic.
		"selector": {
			"criteria": [
				{
					"type": "ETH_SRC",
					"mac": mac_add
				}
			]
		}
	     }
	   ]
	}
	
	urlToPost = "http://192.168.115.131:8181/v1/flows?appId=org.onosproject.fwd"
	print("urlToPost = {0}".format(urlToPost))
	resp = requests.post(urlToPost, data=json.dumps(blockData), auth=('onos', 'rocks'))
	print("response is {0}".format(resp))
		


def ONOS(anomalyIP):
	# First get a list of all hosts.
	# Create dictionary mapping from IP to host switch.
	ipToSwitchMap = {}
	ipToSwitchPortMap = {}
	
	hosts = requests.get('http://192.168.115.131:8181/onos/v1/hosts', auth=('onos', 'rocks'))
	
	host_json = hosts.json()
	for host in host_json['hosts']:
		IP = host['ipAddresses'][0]
		switch = host['locations'][0]['elementId']
		
		ipToSwitchMap[IP] = switch
	
	# Send a request to ONOS to drop traffic for that anomalyIP from the
	# switch the bad device is connected on.
	print("Send to ONOS: Need to block {0}".format(anomalyIP))
	# Configure parameters needed for POST request
	blockData = {
		"priority": 40000,
		"timeout": 0,
		"isPermanent": true,
		"deviceId": "of:000000000000003d",
		"treatment": {},  # blank treatment means drop traffic.
		"selector": {
			"criteria": [
				{
					"type": "IN_PORT",
					"port": 2
				},
				{
					"type": "ETH_SRC",
					"mac": "82:55:FD:61:20:11"
				}
			]
		}
	}
	
	urlToPost = "http://192.168.115.131:8181/onos/v1/flows/{0}?appId=org.onosproject.fwd".format(ipToSwitchMap[anomalyIP])
	print("urlToPost = {0}".format(urlToPost))
	resp = requests.post(urlToPost, data=json.dumps(blockData), auth=('onos', 'rocks'))
	print("response is {0}".format(resp))
	
def Model_IDS_Load():

	# path of saved model
	path = '/home/ubuntu/spark-3.2.0-bin-hadoop3.2/python/ModelDecisionTree'
	# Load model
	model = pipemodel.load(os.path.join(path, 'pipelineModel2'))

	return model
	
    
def print_data(df, epoch_id):
	df_ip = df.select('IPV4_SRC_ADDR').collect()
	drop_col = ('IPV4_SRC_ADDR','L7_PROTO', 'SRC_TO_DST_SECOND_BYTES', 'DST_TO_SRC_SECOND_BYTES')
	data = df.drop(*drop_col)
	model = Model_IDS_Load()
	pre_data = model.transform(data)
	
	dict_ip_count = {
		"10.0.0.1": 0,
		"10.0.0.2": 0,
		"10.0.0.3": 0,
		"10.0.0.4": 0,
		"10.0.0.5": 0,
		"10.0.0.6": 0,
		"10.0.0.7": 0,
		"10.0.0.8": 0,
		"10.0.0.9": 0,
		"10.0.0.10": 0,
		"10.0.0.11": 0,
		"10.0.0.12": 0
	}

	df_pred = pre_data.select('prediction').collect()
	for i in range(df.count()):
		for label in df_pred[i]:
			for ip in df_ip[i]:
				for x, y in dict_ip_count.items():
					if ip == x and label == 1:
						y += 1
						dict_ip_count.update({x: y})
	print("---------------------------------------------")
	for x, y in dict_ip_count.items():
		print(x, y)
	print("--------------------------------------------")
					
				
if __name__ == "__main__":
	df = load_data()

	''' rename column dataframe '''
	oldColumns = df.schema.names
	newColumns = ["IPV4_SRC_ADDR","PROTOCOL","L7_PROTO","IN_BYTES","OUT_BYTES","IN_PKTS","OUT_PKTS","FLOW_DURATION_MILLISECONDS","TCP_FLAGS","CLIENT_TCP_FLAGS", "SERVER_TCP_FLAGS","L7_PROTO_RISK","DURATION_IN","DURATION_OUT","LONGEST_FLOW_PKT","SHORTEST_FLOW_PKT","MIN_IP_PKT_LEN","MAX_IP_PKT_LEN","SRC_TO_DST_SECOND_BYTES", "DST_TO_SRC_SECOND_BYTES","RETRANSMITTED_IN_BYTES","RETRANSMITTED_IN_PKTS","RETRANSMITTED_OUT_BYTES","RETRANSMITTED_OUT_PKTS","SRC_TO_DST_AVG_THROUGHPUT", "DST_TO_SRC_AVG_THROUGHPUT","NUM_PKTS_UP_TO_128_BYTES","NUM_PKTS_128_TO_256_BYTES","NUM_PKTS_256_TO_512_BYTES","NUM_PKTS_512_TO_1024_BYTES","NUM_PKTS_1024_TO_1514_BYTES", "TCP_WIN_MAX_IN","TCP_WIN_MAX_OUT","ICMP_TYPE","ICMP_IPV4_TYPE","DNS_QUERY_ID","DNS_QUERY_TYPE","DNS_TTL_ANSWER","FTP_COMMAND_RET_CODE"]
	
	df = reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), df)
	
	net_stream = df \
	    .writeStream \
	    .foreachBatch(print_data) \
	    .trigger(processingTime='30 seconds') \
	    .start()
	    
	net_stream.awaitTermination()

