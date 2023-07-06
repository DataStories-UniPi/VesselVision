"""
kafka_multiprocess_v04.py
"""
import sys, os
import csv, json
import pandas as pd
import numpy as np
import pdb

from kafka import KafkaConsumer, KafkaProducer, TopicPartition, OffsetAndMetadata


# IMPORT SCRIPT HELPER FUNCTIONS & CONFIGURATION PARAMETERS
sys.path.append(os.path.join(os.path.dirname(__file__), 'lib'))

from lib.kafka_config_c_p_v01 import *
from lib.helper import get_rounded_timestamp, get_aligned_location, adjust_buffers, data_output, readjust_sensors, send_to_kafka_topic
from lib.kafka_update_buffer_v03 import update_buffer, calculate_vessels_cri
from lib.kafka_fun_aux import LoadingBar, StartServer, KafkaTopics, KProducer


# PARALLELIZING MODULES
import logging
import multiprocessing



def init_log_output(consumer_num):
	if CFG_NUM_CONSUMERS == "None" or consumer_num == 0:
		if os.path.isfile(CFG_WRITE_FILE):
			print(CFG_WRITE_FILE, 'File Already Exists... Deleting...')
			os.remove(CFG_WRITE_FILE)


def init_KConsumer():
	if CFG_NUM_CONSUMERS == "None" or CFG_CONSUMERS_EQUAL_TO_PARTITIONS == 'no' or CFG_TOPIC_PARTITIONS != CFG_NUM_CONSUMERS:
		"""Consumer - Reads from all topics"""
		consumer = KafkaConsumer(
			CFG_TOPIC_NAME, 
			bootstrap_servers='localhost:9092', group_id='VesselVision_Consumer',
			auto_offset_reset='latest', enable_auto_commit=False, 
			max_poll_interval_ms=10000
		)
	
	elif CFG_CONSUMERS_EQUAL_TO_PARTITIONS == 'yes' and CFG_TOPIC_PARTITIONS == CFG_NUM_CONSUMERS:
		"""Consumer k reads from the k partition - Assign each k consumer to the k partition """
		consumer = KafkaConsumer(
			bootstrap_servers='localhost:9092', group_id='VesselVision_Consumer',
			auto_offset_reset='latest', enable_auto_commit=False, 
			max_poll_interval_ms=10000
		)
		consumer.subscribe(topics=(CFG_TOPIC_NAME,))
	
	else:
		print('Check Configuration Parameters for #Consumers')

	return consumer


def init_KProducer():
	if CFG_SAVE_TO_TOPIC:
		savedata_producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
	else:
		savedata_producer = None

	return savedata_producer


def KConsumer(consumer_num, CFG_TOPIC_PARTITIONS):
	"""
		Start Consumer
	"""
	LoadingBar(15, desc="Starting Kafka Consumer ...")

	# ====================	INITIALIZING AUXILIARY FILES	====================
	init_log_output(consumer_num)

	# ====================	INSTANTIATE A KAFKA CONSUMER	====================
	consumer = init_KConsumer()

	# ============	INSTANTIATE A KAFKA PRODUCER (FOR DATA OUTPUT)	============
	savedata_producer = init_KProducer()

	# ========================================	NOW THE FUN BEGINS	========================================
	with open(CFG_WRITE_FILE, 'a') as fw2:
		fwriter = csv.writer(fw2)
		fwriter.writerow(['ts', 'message'])
		print('CSV File Writer Initialized...')


		# 0.	INITIALIZE THE BUFFERS
		object_pool = pd.DataFrame(data=[], columns=CFG_BUFFER_COLUMN_NAMES)	# create dataframe which keeps all the messages			
		pending_time = None

		stream_active_pairs, stream_inactive_pairs, stream_pairs_cri = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

		# 0.5.	LOAD VCRA INSTANCE
		vcra_model = pd.read_pickle(CFG_VCRA_MODEL_PATH).iloc[0].instance

		# 1.	LISTEN TO DATASTREAM
		while True:
			message_batch = consumer.poll()

			# Commit Offsets (At-Most-Once Behaviour)
			# consumer.commit_async()		

			for topic_partition, partition_batch in message_batch.items():
				for message in partition_batch:
					# Print Incoming Message (and Test for Consistency)
					print('Incoming Message')
					print ("c{0}:t{1}:p{2}:o{3}: key={4} value={5}".format(consumer_num, message.topic, message.partition, message.offset, message.key, message.value))
					
					# Decode Message
					msg = json.loads(message.value.decode('utf-8'))
					fwriter.writerow([message.timestamp, msg])

					'''
						* Get the Current Datapoint's Timestamp
						* Get the Pending Timestamp (if not initialized)
					'''
					
					# Kafka Message Timestamp is in MilliSeconds 
					if pending_time is None:
						pending_time = get_rounded_timestamp(message.timestamp, base=CFG_DESIRED_ALIGNMENT_RATE_SEC, mode=CFG_ALIGNMENT_MODE, unit='ms') 

					print ("\nCurrent Timestamp: {0} ({1})".format(message.timestamp, pd.to_datetime(message.timestamp, unit='ms')), sep='\t|\t', end='')		
					print ('Pending Timestamp: {0} ({1})'.format(pending_time, pd.to_datetime(pending_time, unit='s')), end='\n\n')

					'''
					If the time is right:
						* Discover Vessel Encounters up to ```curr_time```
						* Save (or Append) the timeslice to the ```kafka_aligned_data_*.csv``` file
						* Save the Discovered Vessel Encounters 
					'''
					if message.timestamp // 10**3 > pending_time:
						# Completing Missing Information
						# Fill NaN values with median for each object
						object_pool.loc[:, CFG_CONSUMER_SPEED_NAME] = object_pool[CFG_CONSUMER_SPEED_NAME].fillna(object_pool.groupby(CFG_PRODUCER_KEY)[CFG_CONSUMER_SPEED_NAME].transform('median'))
						
						# Create the Timeslice
						# Interpolate Points
						timeslice = object_pool.groupby(
							CFG_PRODUCER_KEY, group_keys=False
						).apply(
							lambda l: get_aligned_location(l, pending_time, temporal_name=CFG_PRODUCER_TIMESTAMP_NAME, temporal_unit=CFG_PRODUCER_TIMESTAMP_UNIT, mode=CFG_ALIGNMENT_MODE)
						).reset_index(drop=True)

						# Correct Speed/Course Measurements
						timeslice.set_index(CFG_PRODUCER_KEY, inplace=True)
						timeslice.loc[:, [CFG_CONSUMER_SPEED_NAME, CFG_CONSUMER_COURSE_NAME]] = timeslice.groupby(
							level=0, group_keys=False
						).apply(
							lambda l: readjust_sensors(
								pending_time, 
								l[CFG_CONSUMER_COORDINATE_NAMES], 
								object_pool.loc[object_pool[CFG_PRODUCER_KEY] == l.name],
								feats = [CFG_CONSUMER_SPEED_NAME, CFG_CONSUMER_COURSE_NAME]
							)
						)
						timeslice.reset_index(inplace=True)

						# Create DataFrame Views
						# timeslice_moving = timeslice.loc[timeslice[CFG_CONSUMER_SPEED_NAME].between(CFG_THRESHOLD_MIN_SPEED, CFG_THRESHOLD_MAX_SPEED, inclusive='neither')].copy()

						print ('\t\t\t----- Timeslice Created -----')
						print (timeslice.astype(str), end='\n\n')
						print ('\t\t\t----- (Previous) Active Encountering Pairs -----')
						print (stream_active_pairs.iloc[:,:-2].astype(str), end='\n\n')
						print ('\t\t\t----- (Previous) Closed Encountering Pairs -----')
						print (stream_inactive_pairs.iloc[:,:-2].astype(str), end='\n\n')

						# Calculate Collision Risk
						stream_active_pairs, stream_inactive_pairs, active_pairs_cri = calculate_vessels_cri(pending_time, timeslice, stream_active_pairs, stream_inactive_pairs, verbose=True, vcra_model=vcra_model)

						# Add Encountering Vessels' CRI to Historic List 
						stream_pairs_cri = pd.concat((stream_pairs_cri, active_pairs_cri), ignore_index=False)

						print ('\t\t\t----- (Current) Active Encountering Pairs -----')
						print (f'{stream_active_pairs.iloc[:,:-2].astype(str)=}', end='\n\n')
						print ('\t\t\t----- (Current) Closed Encountering Pairs -----')
						print (f'{stream_inactive_pairs.iloc[:,:-2].astype(str)=}', end='\n\n')
						print ('\t\t\t----- (Current) Encountering Vessels CRI -----')
						print(f'{stream_pairs_cri=}', end='\n\n')

						## Data Output
						data_output(
							savedata_producer, pending_time * 10**3,
							timeslice, 
							stream_active_pairs.iloc[:,:-2].reset_index(), 
							stream_inactive_pairs.iloc[:,:-2].reset_index(), 
							stream_pairs_cri.astype({'own_geometry':str, 'target_geometry':str}), 
							alignment_res_topic_name=CFG_ALIGNMENT_RESULTS_TOPIC_NAME, 
							encounters_topic_name=CFG_ENC_RESULTS_TOPIC_NAME, 
							vcra_topic_name=CFG_CRI_RESULTS_TOPIC_NAME
						)

						# Adjust Buffers and Pending Timestamp
						# object_pool = pd.concat((object_pool, timeslice), ignore_index=True)
						object_pool, pending_time, timeslice = adjust_buffers(pending_time, object_pool.copy(), CFG_PRODUCER_TIMESTAMP_NAME)
						pending_time = get_rounded_timestamp(message.timestamp, base=CFG_DESIRED_ALIGNMENT_RATE_SEC, mode=CFG_ALIGNMENT_MODE, unit='ms') 
						
					'''
					In any case, Update the Objects' Buffer 
					'''			
					oid, ts, lon, lat = msg[CFG_PRODUCER_KEY], msg[CFG_PRODUCER_TIMESTAMP_NAME], msg[CFG_CONSUMER_COORDINATE_NAMES[0]], msg[CFG_CONSUMER_COORDINATE_NAMES[1]] # parameters for function update_buffer must be int/float
					object_pool = update_buffer(object_pool, oid, ts, lon, lat, **{k:msg[k] for k in CFG_BUFFER_OTHER_FEATURES})


				# Send Updated Buffer to **CFG_BUFFER_DATA_TOPIC_NAME** 
				send_to_kafka_topic(savedata_producer, CFG_BUFFER_DATA_TOPIC_NAME, message.timestamp, object_pool)


			# Commit Offsets (At-Least-Once Behaviour)
			consumer.commit_async()

			
def main():
	# StartServer() # start Zookeeper & Kafka
	# KafkaTopics() # Delete previous topic & Create new

	print('Start %d Consumers & 1 Producer with %d partitions' % (CFG_NUM_CONSUMERS, CFG_TOPIC_PARTITIONS))

	jobs = []

	job = multiprocessing.Process(target=StartServer) # 	Job #0: Start Kafka & Zookeeper
	jobs.append(job)

	job = multiprocessing.Process(target=KafkaTopics, args=(CFG_TOPIC_NAME,)) # 	Job #1: Delete previous kafka topic & Create new one (Simulating a DataStream via CSV files)
	jobs.append(job)

	job = multiprocessing.Process(target=KafkaTopics, args=(CFG_CRI_RESULTS_TOPIC_NAME,)) # 	Job #2: Delete previous kafka topic & Create new one (Encountering Vessels' CRI)
	jobs.append(job)

	job = multiprocessing.Process(target=KafkaTopics, args=(CFG_ENC_RESULTS_TOPIC_NAME,)) # 	Job #3: Delete previous kafka topic & Create new one (Encountering Vessels' Pairs)
	jobs.append(job)

	job = multiprocessing.Process(target=KafkaTopics, args=(CFG_BUFFER_DATA_TOPIC_NAME,)) # 	Job #4: Delete previous kafka topic & Create new one (AIS Points in Buffer)
	jobs.append(job)

	job = multiprocessing.Process(target=KafkaTopics, args=(CFG_ALIGNMENT_RESULTS_TOPIC_NAME,)) # 	Job #5: Delete previous kafka topic & Create new one (Alignment Results Output Topic)
	jobs.append(job)

	for i in range(CFG_NUM_CONSUMERS): # Create different consumer jobs
		job = multiprocessing.Process(target=KConsumer, args=(i,CFG_TOPIC_PARTITIONS))
		jobs.append(job)

	job = multiprocessing.Process(target=KProducer) # 	Job #4: Start Producer
	jobs.append(job)

	for job in jobs: # 	Start the Threads
		job.start()

	for job in jobs: # 	Join the Threads
		job.join()

	print("Done!")


if __name__ == "__main__":
	logging.basicConfig(
		format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
		level=logging.INFO
	)
	main()
