"""
kafka_update_buffer_v03.py
Update buffer
"""
import os, sys
import numpy as np
import pandas as pd
import geopandas as gpd
import shapely as shp 

from helper import haversine, initial_compass_bearing
import encounters

from kafka_config_c_p_v01 import *


def update_object_pool(object_pool, cand_record):
	return pd.concat((object_pool, pd.DataFrame(cand_record)), ignore_index=True)


def update_buffer(object_pool, oid, ts, lon, lat, **kwargs):
	'''
		Update the Objects' buffer, given a new location record.

		Input
		-----
		* object_pool:	The Records for all objects (Universal Buffer)
		* oid: 			The identifier of the object
		* ts:			The timestamp of the record
		* lon, lat:		The location of the record (longitude, latitude)
		* **kwargs:		Other features to be included to the buffer

		Output
		------
		* object_pool:		The Records for all objects (Universal Buffer)
	'''
	object_buffer = object_pool.loc[object_pool[CFG_PRODUCER_KEY] == oid].copy()
	cand_record = {
		CFG_PRODUCER_KEY: [oid], 
		CFG_PRODUCER_TIMESTAMP_NAME: [ts], 
		CFG_CONSUMER_COORDINATE_NAMES[0]: [lon], 
		CFG_CONSUMER_COORDINATE_NAMES[1]: [lat], 
		**{k:[v] for k, v in kwargs.items()}
	}

	if object_buffer.empty: # if this record is the first point of the vessel
		print('----- Cannot inter-/extra-polate for vessel: {0} -----'.format(oid))
		print('----- This is the first record for this vessel -----')
		return update_object_pool(object_pool, cand_record)  	# append row to the ```objects_pool``` dataframe
	
	object_latest_record = object_buffer.loc[
		object_buffer[CFG_PRODUCER_TIMESTAMP_NAME] == object_buffer[CFG_PRODUCER_TIMESTAMP_NAME].max()
	]	# The latest record for mmsi: %oid
	
	dt = ts - object_latest_record[CFG_PRODUCER_TIMESTAMP_NAME].values[0]	# Calculate the dt of the incoming record from the one of the latest
	
	if dt < 1:	# if this record is less than 1 sec from the previous
		print('----- Invalid record: dt < 1sec -----')
		assert (dt < 0), f'----- Records for each vessel must be sorted by timestamp ({dt=}; {object_latest_record=}) -----'
		return object_pool
	
	if dt > 2*CFG_DESIRED_ALIGNMENT_RATE_SEC:
		print('----- Cannot inter-/extra-polate for vessel: {0} -----'.format(oid))
		print('----- Delete all the previous records: dt > {0}sec and id: {1} -----'.format(2*CFG_DESIRED_ALIGNMENT_RATE_SEC, oid))
		object_pool.drop(object_pool.loc[object_pool[CFG_PRODUCER_KEY] == oid].index, inplace=True)
		object_pool = update_object_pool(object_pool, cand_record)  	# append row to the ```objects_pool``` dataframe
	
	else:
		point_b, point_a = shp.geometry.Point(lon, lat), shp.geometry.Point(
			object_latest_record[CFG_CONSUMER_COORDINATE_NAMES[0]].values[0], object_latest_record[CFG_CONSUMER_COORDINATE_NAMES[1]].values[0]
		)

		dist_m = haversine(point_a, point_b) / 1.852					# Calculate Haversine Disance (in meters)
		oid_point_speed = dist_m * (3600 / dt)							# Calculate (Momentrary) Object Speed
		oid_point_course = initial_compass_bearing(point_a, point_b)	# Calculate Initial Compass Bearing (in degrees)

		if oid_point_speed > CFG_THRESHOLD_MAX_SPEED:
			print('----- Cannot inter-/extra-polate for vessel: {0} -----'.format(oid))
			print('----- Invalid speed record ({0} m/s) -----'.format(oid_point_speed))
		else:
			cand_record[CFG_CONSUMER_SPEED_NAME], cand_record[CFG_CONSUMER_COURSE_NAME] = [oid_point_speed], [oid_point_course]
			object_pool = update_object_pool(object_pool, cand_record)  	# append row to the ```objects_pool``` dataframe

			if CFG_ALIGNMENT_MODE == 'extra':
				# Adjust Buffer (Keep only ```k``` latest positions) -- Query Works for Extrapolation (so far)
				object_pool.drop(object_pool.loc[object_pool[CFG_PRODUCER_KEY] == oid].iloc[:-3].index, inplace=True)				
					
	return object_pool


def calculate_vessels_cri(curr_time, timeslice_moving, stream_active_pairs, stream_inactive_pairs, verbose=True, stationary_speed_threshold=1, vcra_model=None):
	timeslice = gpd.GeoDataFrame(
		timeslice_moving.copy(), 
		columns=CFG_BUFFER_COLUMN_NAMES, 
		geometry=gpd.points_from_xy(
			timeslice_moving[CFG_CONSUMER_COORDINATE_NAMES[0]], 
			timeslice_moving[CFG_CONSUMER_COORDINATE_NAMES[1]]
		), 
		crs=CFG_DATASET_CRS
	)	# create dataframe which keeps all the messages			

	stream_current_pairs = encounters.current_pairs(
		curr_time, 	timeslice, 
		oid_name=CFG_PRODUCER_KEY, coords=CFG_CONSUMER_COORDINATE_NAMES, diam=CFG_DISTANCE_THRESHOLD
	)
	# print(f'\t{stream_current_pairs=}')
	# print(f'\t{len(stream_current_pairs)=}')

	# Case #1 - ```active_pairs``` is empty; All Current Pairs are encountering "candidates"
	if stream_active_pairs.empty:
		stream_active_pairs = stream_current_pairs.copy()
		return stream_active_pairs, stream_inactive_pairs, pd.concat(
			{curr_time: pd.DataFrame([], columns=CFG_CRI_DATASET_FEATURES)}, names=[CFG_PRODUCER_TIMESTAMP_NAME,]
		)

	''' 
		Get the Current view of previous vs. current pairs; Columns: 
		< dist_prev, start_prev, end_prev, kinematics_own_prev, kinematics_target_prev, 
			dist, start, end, kinematics_own, kinematics_target> 
	'''
	stream_current_pairs = pd.merge(
		stream_active_pairs, stream_current_pairs.copy(), how='outer', left_index=True, right_index=True, suffixes=('_prev', '')
	)

	stream_active_pairs, stream_inactive_pairs = encounters.encountering_vessels_timeslice(
			stream_current_pairs, stream_inactive_pairs, CFG_TEMPORAL_THRESHOLD
	)

	'''    
		For the ```active_pairs``` which satisfy ** temporal constraints **, calculate the vessels' CRI
	'''
	active_pairs_cri = stream_active_pairs.loc[stream_active_pairs.end - stream_active_pairs.start >= CFG_TEMPORAL_THRESHOLD].apply(
		lambda l: [
			*encounters.get_kinematics(l.kinematics_own), 
			*encounters.get_kinematics(l.kinematics_target),
			*encounters.calculate_cri(l.kinematics_own, l.kinematics_target, stationary_speed_threshold=stationary_speed_threshold, vcra_model=vcra_model)
		], axis=1
	).values.tolist()

	return stream_active_pairs, stream_inactive_pairs, pd.concat(
        {curr_time: pd.DataFrame(active_pairs_cri, columns=CFG_CRI_DATASET_FEATURES)}, names=[CFG_PRODUCER_TIMESTAMP_NAME,]
    )
