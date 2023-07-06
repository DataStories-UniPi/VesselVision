import geopandas as gpd
import pandas as pd
import numpy as np
import datetime

import shapely 
from scipy import interpolate
from collections import deque
from tqdm import tqdm
import os, json

from kafka_config_c_p_v01 import *


def haversine(p_1, p_2):
	'''
		Calculate the haversine distance between two points.

		Input
		=====
			* p_1: The coordinates of the first point (```shapely.geometry.Point``` instance)
			* p_1: The coordinates of the second point (```shapely.geometry.Point``` instance)

		Output
		=====
			* The haversine distance between two points in Kilometers
	'''
	lon1, lat1, lon2, lat2 = map(np.deg2rad, [p_1.x, p_1.y, p_2.x, p_2.y])

	dlon = lon2 - lon1
	dlat = lat2 - lat1
	a = np.power(np.sin(dlat * 0.5), 2) + np.cos(lat1) * np.cos(lat2) * np.power(np.sin(dlon * 0.5), 2)
	
	return 2 * 6371.0088 * np.arcsin(np.sqrt(a))


def initial_compass_bearing(point1, point2):
	"""
		Calculate the initial compass bearing between two points.

		$ \theta = \atan2(\sin(\Delta lon) * \cos(lat2), \cos(lat1) * \sin(lat2) - \sin(lat1) * \cos(lat2) * \cos(\Delta lon)) $

		Input
		=====
		  * point1: shapely.geometry.Point Instance
		  * point2: shapely.geometry.Point Instance

		Output
		=====
			The bearing in degrees
	"""
	lat1 = np.radians(point1.y)
	lat2 = np.radians(point2.y)
	delta_lon = np.radians(point2.x - point1.x)

	x = np.sin(delta_lon) * np.cos(lat2)
	y = np.cos(lat1) * np.sin(lat2) - (np.sin(lat1) * np.cos(lat2) * np.cos(delta_lon))
	initial_bearing = np.arctan2(x, y)

	# Now we have the initial bearing but math.atan2 return values from -180 to + 180
	# Thus, in order to get the range to [0, 360), the solution is:
	initial_bearing = np.degrees(initial_bearing)
	compass_bearing = (initial_bearing + 360) % 360
	return compass_bearing


def readjust_sensors(timestamp, aligned_location, actual_locations, feats):
	prev_location = actual_locations.loc[
		actual_locations[CFG_PRODUCER_TIMESTAMP_NAME] == actual_locations[CFG_PRODUCER_TIMESTAMP_NAME].max()
	]	# The latest record for mmsi: %oid

	point_b, point_a = shapely.geometry.Point(*aligned_location.values[0]), shapely.geometry.Point(
		*prev_location[CFG_CONSUMER_COORDINATE_NAMES].values[0]
	)

	dist_m = haversine(point_a, point_b) / 1.852					# Calculate Haversine Disance (in meters)
	dt = timestamp - prev_location[CFG_PRODUCER_TIMESTAMP_NAME].values[0]	# Calculate the dt of the incoming record from the one of the latest

	if dt != 0:
		oid_point_speed = dist_m * (3600 / dt)							# Calculate (Momentrary) Object Speed
	else:
		oid_point_speed = prev_location[CFG_CONSUMER_SPEED_NAME].values[0]

	oid_point_course = initial_compass_bearing(point_a, point_b)	# Calculate Initi.l Compass Bearing (in degrees)
	return pd.Series([oid_point_speed, oid_point_course], index=[CFG_CONSUMER_SPEED_NAME, CFG_CONSUMER_COURSE_NAME])


def get_rounded_timestamp(timestamp, unit='ms', base=60, mode='inter'):
	timestamp_s = pd.to_datetime(timestamp, unit=unit).timestamp()

	if mode=='inter':
		return int(base * np.floor(np.float(timestamp_s)/base))
	elif mode=='extra':
		return int(base * np.ceil(np.float(timestamp_s)/base))
	else:
		raise Exception('Invalid Alignment Mode.')


def get_aligned_location(df, timestamp, temporal_name='ts', temporal_unit='s', mode='inter'):
	x = df[temporal_name].values.astype(np.int64)
	y = df.values
	
	try:
		'''
		**Special Use-Case #1**: If the **only** record on the object's buffer happens to be at 
		the pending timestamp, return the aforementioned record, regardless of the alignment mode.
		'''
		if len(df)==1 and not df.loc[df[temporal_name] == timestamp].empty:
			# Return the Resulted Record
			return df.copy()

		'''
		**Casual Use-Case**: Otherwise, inter-/extra-polate to the pending timestamp, 
		according to the alignment mode's specifications.
		'''
		if mode == 'inter':
			f = interpolate.interp1d(x, y, kind='linear', axis=0)
		elif mode == 'extra':
			f = interpolate.interp1d(x, y, kind='linear', fill_value='extrapolate', axis=0)
		else:
			raise Exception('Invalid Alignment Mode.')
	
		''' 
		Return the Resulted Record 
		'''
		return pd.DataFrame(f(timestamp).reshape(1,-1), columns=df.columns)     
	
	except ValueError as ve:
		'''
		**Special Use-Case #2**: If no alignment is possible, return an empty DataFrame
		'''
		return df.iloc[0:0].copy()


def reinitialize_aux_variables(timestamp):
	pending_time = timestamp
	timeslice = pd.DataFrame(columns=CFG_BUFFER_COLUMN_NAMES)

	return pending_time, timeslice	


def adjust_buffers(old_pending_time, object_pool, temporal_name):
	# 2. Crop the Objects' Pool for the next cycle
	object_pool = object_pool.loc[object_pool[temporal_name].between(old_pending_time-CFG_DESIRED_ALIGNMENT_RATE_SEC, old_pending_time+CFG_DESIRED_ALIGNMENT_RATE_SEC, inclusive='both')]
	# 1. Re-initialize auxiliary variables
	new_pending_time, timeslice = reinitialize_aux_variables(old_pending_time)

	return object_pool, new_pending_time, timeslice


def checkpoint_csv(timeslice, active_pairs, inactive_pairs, pairs_cri):
	'''
	Save Data to CSV File
	'''
	path_encounter_pairs = os.path.join(
		CFG_CSV_OUTPUT_DIR, f'kafka_{CFG_ALIGNMENT_MODE}_ecountering_pairs_params_t={CFG_TEMPORAL_THRESHOLD}_theta={CFG_DISTANCE_THRESHOLD}_dataset_{CFG_DATASET_NAME}.pickle'
	)
	path_timeslice = os.path.join(
		CFG_CSV_OUTPUT_DIR, f'kafka_{CFG_ALIGNMENT_MODE}_aligned_data_params_rate={CFG_DESIRED_ALIGNMENT_RATE_SEC}_dataset_{CFG_DATASET_NAME}.csv'
	)
	path_vessels_cri = os.path.join(
		CFG_CSV_OUTPUT_DIR, f'kafka_{CFG_ALIGNMENT_MODE}_vcra_results_params_t={CFG_TEMPORAL_THRESHOLD}_theta={CFG_DISTANCE_THRESHOLD}_dataset_{CFG_DATASET_NAME}.pickle'
	)

	# Save VCRA Results
	# pairs_cri.to_csv(path_vessels_cri, index=True, header=True)
	pairs_cri.to_pickle(path_vessels_cri)

	# Save Encountering Results
	pd.concat(
		[active_pairs, inactive_pairs]
	# ).to_csv(path_encounter_pairs, index=True, header=True)
	).to_pickle(path_encounter_pairs)
	
	# Save Aligned Timeslice Results
	if os.path.isfile(path_timeslice):
		timeslice.to_csv(path_timeslice, mode='a', index=False, header=False)
	else:
		timeslice.to_csv(path_timeslice, index=False, header=True)


def send_to_kafka_topic(kafka_producer, kafka_topic, timestamp, df):
	# Creating Key for Kafka Messages
	key_time = json.dumps(timestamp).encode('utf-8')

	# Save Encoded DataFrame to Kafka Topic
	kafka_producer.send(
		kafka_topic, key=key_time, value=df.to_json(orient='table').encode('utf-8'), timestamp_ms=timestamp
	)
	return key_time


def checkpoint_kafka(kafka_producer, timestamp, timeslice, active_pairs, inactive_pairs, pairs_cri, alignment_res_topic_name, encounters_topic_name, vcra_topic_name):
	'''
	Save Data to Kafka Topic
	'''
	# Save Produced Timeslice to Kafka Topic ```CFG_ALIGNMENT_RESULTS_TOPIC_NAME```
	key_time = send_to_kafka_topic(
		kafka_producer=kafka_producer, kafka_topic=alignment_res_topic_name, timestamp=timestamp, df=timeslice
	)

	# Save Produced EvolvingClusters to Kafka Topic ```CFG_ENC_RESULTS_TOPIC_NAME```
	data = {
		'active': active_pairs.to_json(orient='table'), 
		'closed': inactive_pairs.to_json(orient='table')
	}
	kafka_producer.send(
		encounters_topic_name, key=key_time, value=json.dumps(data).encode('utf-8'), timestamp_ms=timestamp
	)

	# Save Produced Timeslice to Kafka Topic ```CFG_ALIGNMENT_RESULTS_TOPIC_NAME```
	key_time = send_to_kafka_topic(
		kafka_producer=kafka_producer, kafka_topic=vcra_topic_name, timestamp=timestamp, df=pairs_cri
	)


def data_output(kafka_producer, timestamp, timeslice, active_pairs, inactive_pairs, pairs_cri, **kwargs):
	if CFG_SAVE_TO_FILE:
		checkpoint_csv(timeslice, active_pairs, inactive_pairs, pairs_cri)
	if CFG_SAVE_TO_TOPIC:
		checkpoint_kafka(kafka_producer, timestamp, timeslice, active_pairs, inactive_pairs, pairs_cri, **kwargs)
