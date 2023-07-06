import urllib
import itertools
import time, datetime

import numpy as np
import pandas as pd
import geopandas as gpd

import bokeh
import bokeh.models as bokeh_models
import bokeh.layouts as bokeh_layouts
import bokeh.palettes as bokeh_palettes
import bokeh.io as bokeh_io
from bokeh.driving import linear

import sys, os
sys.path.append(os.path.join(os.path.expanduser('~'), 'Documents'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lib'))

from st_visions.st_visualizer import st_visualizer
import st_visions.express as viz_express
import st_visions.geom_helper as viz_helper 
import st_visions.callbacks as viz_cbs

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic

from monitor.vessel_positions_json import get_data, toggle_renderers, render_items, track_selected, APP_ROOT
from shapely import wkt, wkb

import live_feed
import historic_analytics_grid
import historic_analytics_vessel
from kafka_config_c_p_v01 import *



def main():
	CFG_USER_ID = hash(np.random.randint(1e9))
	CFG_PRODUCER_KEY_CMAP = bokeh_palettes.Category10[10]

	title = 'VesselVisions - SIGSPATIAL\'23 Demo Track'
	

	consumer_vcra = KafkaConsumer(f'criresults{CFG_TOPIC_SUFFIX}',
                     bootstrap_servers=['localhost:9092'],
                     group_id=f'VesselVisions_Consumer_VCRA_#{CFG_USER_ID}',
                     auto_offset_reset='latest',
					 max_poll_interval_ms=20000,
					 enable_auto_commit=True,
					 max_poll_records=1)
					 
	consumer_enc  = KafkaConsumer(f'encounteresults{CFG_TOPIC_SUFFIX}',
                     bootstrap_servers=['localhost:9092'],
                     group_id=f'VesselVisions_Consumer_Encounters_#{CFG_USER_ID}',
                     auto_offset_reset='latest',
					 max_poll_interval_ms=20000,
					 enable_auto_commit=True,
					 max_poll_records=1)

	consumer_op   = KafkaConsumer(f'alignedata{CFG_TOPIC_SUFFIX}',
                     bootstrap_servers=['localhost:9092'],
                     group_id=f'VesselVisions_Consumer_ObjectPool_#{CFG_USER_ID}',
                     auto_offset_reset='latest',
					 max_poll_interval_ms=20000,
					 enable_auto_commit=True,
					 max_poll_records=1)


	@linear()
	def update_live_feed(step):
		# Flusing old Data
		st_viz_old_title = st_viz.figure.title.text
		st_viz.figure.title.text = 'Refreshing...'

		# Track Selected Objects #1 - Fetch at Start in order to Avoid Inconsistencies during the Update Sequence
		tracked_points_ix, tracked_points = st_viz.source.selected.indices, set([])
		tracked_encounters_ix, tracked_encounters = st_viz2.source.selected.indices, set([])
		
		# Fetching New Data
		encounters, cri_data, ais_points, curr_timestamp = get_data(consumer_vcra, consumer_enc, consumer_op)
		
		# Track Selected Objects #1.5 - Find Selected Objects in the Updated Dataset
		if tracked_encounters_ix:
			tracked_encounters = [tuple(i) for i in np.array(st_viz2.source.data['pair'])[tracked_encounters_ix]]
			tracked_encounters_ix = track_selected(encounters, 'pair', tracked_encounters)
			tracked_points = set(itertools.chain.from_iterable(tracked_encounters))


		if tracked_points_ix or tracked_points:
			tracked_points |= set(np.array(st_viz.source.data[CFG_PRODUCER_KEY])[tracked_points_ix])
			tracked_points_ix = track_selected(ais_points, CFG_PRODUCER_KEY, list(tracked_points))

		# Visualize Data; If no new data are fetched, do not refresh (otherwise the view will be flushed)
		##   * Prepare Data for Visualization -- st_viz (AIS Points)
		if not ais_points.empty:
			render_items(ais_points, st_viz)
			st_viz.source.selected.indices = [ais_points.index.get_loc(i) for i in tracked_points_ix]
			
		##   * Prepare Data for Visualization -- st_viz2 (Vessel Encounters)
		if not encounters.empty:
			render_items(encounters, st_viz2)
			st_viz2.source.selected.indices = [encounters.index.get_loc(i) for i in tracked_encounters_ix]

		##   * Prepare Data for Visualization -- st_viz3 (Vessels' CRI)
		# render_items(cri_data, st_viz3)

		## Flush Buffers
		st_viz.canvas_data, st_viz.data = None, None
		st_viz2.canvas_data, st_viz2.data = None, None

		## Restore Title
		if curr_timestamp is not None:
			st_viz.figure.title.text = f'Monitoring Vessel Encounters @{pd.to_datetime(curr_timestamp, unit="ms")} UTC'
		else:
			st_viz.figure.title.text = st_viz_old_title


	@linear()
	def update_spatial_grid(step):    
		# Vessel Encounters
		df_enc_hist = pd.read_pickle(f'./data/kafka_extra_ecountering_pairs_params_t={CFG_TEMPORAL_THRESHOLD}_theta={CFG_DISTANCE_THRESHOLD}_dataset_{CFG_DATASET_NAME}.pickle')
		# CRI Results
		df_vcra_hist = pd.read_pickle(f'./data/kafka_extra_vcra_results_params_t={CFG_TEMPORAL_THRESHOLD}_theta={CFG_DISTANCE_THRESHOLD}_dataset_{CFG_DATASET_NAME}.pickle')

		# Calc avg. CRI for each encounter 
		dataset = historic_analytics_grid.process_encountering_processes(
			df_enc_hist.copy(), 
			df_vcra_hist.copy()
		).reset_index()

		# Update Temporal Filter 
		temp_filter = sp_grid_st_viz2.widgets[-1]
		
		if not dataset.empty:
			# print(f'{temp_filter.start=}; {temp_filter.end=}; {temp_filter.value=}')
			old_value = temp_filter.value
			new_value = tuple(pd.to_datetime(dataset['start'], unit=CFG_PRODUCER_TIMESTAMP_UNIT).agg([np.min, np.max]).values)
		
			# if not dataset.empty:
			temp_filter.start, temp_filter.end = new_value
			
			# Render Geometries
			sp_grid_st_viz2.set_data(dataset.copy())
			render_items(sp_grid_st_viz2.data, sp_grid_st_viz2)

			# Trigger Callbacks; If the starting timestamp is "NaN" (i.e., equal to 0), adjust filter value
			if old_value[1] - old_value[0] <= 1000:
				temp_filter.value = (int(new_value[0]) // 10**6, int(new_value[1]) // 10**6)

		temp_filter.trigger('value_throttled', None, temp_filter.value) 


	@linear()
	def update_vessel_analytics(step):    
		# CRI Results
		df_vcra_hist = pd.read_pickle(f'./data/kafka_extra_vcra_results_params_t={CFG_TEMPORAL_THRESHOLD}_theta={CFG_DISTANCE_THRESHOLD}_dataset_{CFG_DATASET_NAME}.pickle')

		# Aligned Timeslices Results
		df_pts_hist = pd.read_csv(f'./data/kafka_extra_aligned_data_params_rate={CFG_TEMPORAL_THRESHOLD//2}_dataset_{CFG_DATASET_NAME}.csv')
		df_pts_hist = gpd.GeoDataFrame(df_pts_hist, geometry=gpd.points_from_xy(df_pts_hist['lon'], df_pts_hist['lat']), crs=4326)

		# Create Dataset
		dataset = df_pts_hist.set_index(['timestamp', 'mmsi']).join(
			df_vcra_hist.set_index(
				'own_mmsi', append=True)
			.reset_index(
				level=1, drop=True
			).rename_axis(
				[CFG_PRODUCER_TIMESTAMP_NAME, CFG_PRODUCER_KEY]
			)[['target_mmsi', 'ves_cri']],
			how='left'
		).reset_index().astype({CFG_PRODUCER_KEY:int}).astype({CFG_PRODUCER_KEY:str})

		dataset[CFG_PRODUCER_TIMESTAMP_NAME] = pd.to_datetime(dataset[CFG_PRODUCER_TIMESTAMP_NAME], unit=CFG_PRODUCER_TIMESTAMP_UNIT)


		# Update Temporal Filter 
		vessel_id_filter = ves_traj_st_viz.widgets[-3]
		vessel_id_filter.options = [('', 'Select...'), *[(i, i) for i in sorted(dataset[CFG_PRODUCER_KEY].unique())]]

		# Update Temporal Filter 
		temp_filter = ves_traj_st_viz.widgets[-2]
		if not dataset.empty:
			# print(f'{temp_filter.start=}; {temp_filter.end=}; {temp_filter.value=}')
			old_value = temp_filter.value
			new_value = tuple(pd.to_datetime(dataset[CFG_PRODUCER_TIMESTAMP_NAME], unit=CFG_PRODUCER_TIMESTAMP_UNIT).agg([np.min, np.max]).values)
		
			# if not dataset.empty:
			temp_filter.start, temp_filter.end = new_value
			
			# Render Geometries
			ves_traj_st_viz.set_data(dataset.copy())
			# render_items(ves_traj_st_viz.data, ves_traj_st_viz)

			# Trigger Callbacks; If the starting timestamp is "NaN" (i.e., equal to 0), adjust filter value
			if old_value[1] - old_value[0] <= 1000:
				temp_filter.value = (int(new_value[0]) // 10**6, int(new_value[1]) // 10**6)

		temp_filter.trigger('value_throttled', None, temp_filter.value) 




	# This function executes when the user closes the session.
	def close_kafka_topics(session_context):
		print(f'\t\t----- Closing VCRA Kafka Topic  ----- \n')
		consumer_vcra.close()
		print(f'\t\t----- Closing Encountering Vessels\' Kafka Topic  ----- \n')
		consumer_enc.close()
		print(f'\t\t----- Closing Vessels\' Locations Kafka Topic ----- \n')
		consumer_op.close()


	live_feed_grid, [st_viz, st_viz2] = live_feed.build_live_feed()
	historic_spatial_grid, [sp_grid_st_viz, sp_grid_st_viz2] = historic_analytics_grid.build_hist_spatial_grid()
	historic_vessel_trajs, [ves_traj_st_viz,] = historic_analytics_vessel.build_hist_vessel_trajs()

	doc = bokeh_io.curdoc()
	doc.add_root(bokeh_models.widgets.Tabs(
		tabs=[
			bokeh_models.widgets.Panel(child=live_feed_grid, title="Live Feed"),
			bokeh_models.widgets.Panel(child=historic_spatial_grid, title="Analytics (Spatial Grid)"),
			bokeh_models.widgets.Panel(child=historic_vessel_trajs, title="Analytics (Vessels' Trajectories)"),
		]
	))

	doc.add_periodic_callback(update_live_feed, 1500) # Update Tab #1; Period in ms
	doc.add_periodic_callback(update_spatial_grid, 10500) # Update Tab #2; Period in ms
	doc.add_periodic_callback(update_vessel_analytics, 10500) # Update Tab #3; Period in ms
	doc.on_session_destroyed(close_kafka_topics)

	doc.title = title

main()