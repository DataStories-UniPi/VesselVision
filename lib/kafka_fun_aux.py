from datetime import datetime, timezone
from time import sleep
from tqdm import tqdm

import sys, os
import collections

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

import pandas as pd
import numpy as np
import psycopg2
from kafka_config_c_p_v01 import *


def LoadingBar(time_sec, desc):
	for _ in tqdm(range(time_sec), desc=desc):
		sleep(1)
	return None


def StartServer():
	"""
		Start Server
	"""
	# Single SYS Call (V2)
	os.system("{0}/zookeeper-server-start.sh {1}/zookeeper.properties & {0}/kafka-server-start.sh {1}/server.properties".format(CFG_KAFKA_BIN_FOLDER, CFG_KAFKA_CFG_FOLDER))



def KafkaTopics(topic_name):
	def fun_delete_kafka_topic(name):
		"""
			Delete Previous Kafka Topic
		"""
		client = KafkaAdminClient(bootstrap_servers="localhost:9092")
		
		print('Deleting Previous Kafka Topic(s) ...')
		if name in client.list_topics():
			print('Topic {0} Already Exists... Deleting...'.format(name))
			client.delete_topics([name])  # Delete kafka topic
		
		print("List of Topics: {0}".format(client.list_topics()))  # See list of topics


	def fun_create_topic(name):
		"""
			Create Topic
		"""
		print('Create Kafka Topic {0}...'.format(name))
		client = KafkaAdminClient(bootstrap_servers="localhost:9092")
		topic_list = []
		
		print('Create Topic with {0} Partitions and replication_factor=1'.format(CFG_TOPIC_PARTITIONS))
		topic_list.append(NewTopic(name=name, num_partitions=CFG_TOPIC_PARTITIONS, replication_factor=1))
		client.create_topics(new_topics=topic_list, validate_only=False)
		
		print("List of Topics: {0}".format(client.list_topics()))  # See list of topics
		print("Topic {0} Description:".format(name))
		print(client.describe_topics([name]))


	LoadingBar(10, desc="Creating/Cleaning Kafka Topics...")
	fun_delete_kafka_topic(topic_name)
	fun_create_topic(topic_name)


def KProducer():
	"""
		Start Producer
	"""

	LoadingBar(40, desc="Starting Kafka (Input) Producer ...")
	producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
	
	# TimeDelta Queue Definition
	dt = collections.deque(maxlen=2)

	# Pandas CSV File Iterator Definition
	pandas_iter = pd.read_csv(
		CFG_READ_FILE, 
		iterator=True, 
		chunksize=1, 
		sep=CFG_PRODUCER_SEP, 
		dtype=CFG_PRODUCER_DTYPE
	)

	for row in pandas_iter:
		row[CFG_PRODUCER_TIMESTAMP_NAME] = row[CFG_PRODUCER_TIMESTAMP_NAME].apply(lambda x: pd.to_datetime(x, unit=CFG_PRODUCER_TIMESTAMP_UNIT).timestamp())
		
		key_mmsi = row[CFG_PRODUCER_KEY].to_json(orient='records', lines=True).encode('utf-8')
		data = row.reset_index().to_json(orient='records', lines=True).encode('utf-8')
		
		timestamp_s = row[CFG_PRODUCER_TIMESTAMP_NAME].values[0]
		dt.append(timestamp_s)
		
		sleep_dt = np.diff(dt)[0] if len(dt) > 1 else 0
		sleep(max(0.1, sleep_dt / CFG_THROUGHPUT_SCALE))

		# print(data, key_mmsi, timestamp_s, sleep_dt, sep='\n\n', end='\n\n\n')
		producer.send(CFG_TOPIC_NAME, key=key_mmsi, value=data, timestamp_ms=int(timestamp_s*10**3)) # send each csv row to consumer
		
	print('\t\t\t---- Successfully Sent Data to Kafka Topic ----')
	