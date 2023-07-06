import urllib
import numpy as np
import pandas as pd
import geopandas as gpd

import shapely as shp
from shapely import wkt
import itertools
# import psycopg2

import sys, os
import time
import json

sys.path.append(os.path.join(os.path.expanduser('~'), 'Documents', 'st_visions'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lib'))

import geom_helper as viz_helper 
from kafka_config_c_p_v01 import *


# Define Global Variables
APP_ROOT = os.path.dirname(__file__)
DEG2GEO = lambda x: (450 - x) % 360


def fetch_latest_positions(consumer_ais):
	try:
		message_ais = consumer_ais.poll(max_records=1)
		message_ais = next(iter(message_ais.values()))[0]
	except StopIteration as si:
		print(f'\t\t----- Message Poll Failed ----- \nLog Traceback: {si}')
		message_ais = None

	try:
		df_points_ais = pd.read_json(message_ais.value.decode('utf-8'), orient='table')
		df_points_ais.loc[:, 'is_moving'] = df_points_ais[CFG_CONSUMER_SPEED_NAME].apply(lambda l: 'Y' if l > CFG_THRESHOLD_MIN_SPEED else 'N')
		df_points_ais = gpd.GeoDataFrame(df_points_ais, geometry=gpd.points_from_xy(df_points_ais['lon'], df_points_ais['lat']), crs=CFG_DATASET_CRS)
	except Exception as e:
		# if it fails (for some reason) return an empty GeoDataFrame
		print(f'\t\t----- Exception @ ```fetch_latest_positions``` ----- \nLog Traceback: {e}')
		df_points_ais = gpd.GeoDataFrame(data=[], columns=[*CFG_BUFFER_COLUMN_NAMES, 'is_moving', 'geometry'], geometry='geometry', crs=CFG_DATASET_CRS)
	
	return (
		df_points_ais.astype({CFG_PRODUCER_KEY:int})\
					 .set_index(CFG_PRODUCER_TIMESTAMP_NAME, append=True)\
					 .reorder_levels((1,0)),
		message_ais.timestamp if message_ais is not None else None
	)


def __fetch_latest_encounters(message_enc_data, tag):
	try:
		encountering_pairs = pd.read_json(message_enc_data[tag], orient='table')
		encountering_pairs.start = pd.to_datetime(encountering_pairs.start, unit=CFG_PRODUCER_TIMESTAMP_UNIT)
		encountering_pairs.end = pd.to_datetime(encountering_pairs.end, unit=CFG_PRODUCER_TIMESTAMP_UNIT)
		
		emerged_encounters = encountering_pairs.geometry.apply(len) < 2
		encountering_pairs.loc[emerged_encounters, 'geometry'] += encountering_pairs.loc[emerged_encounters, 'geometry']
		encountering_pairs.geometry = encountering_pairs.geometry.apply(lambda x: shp.geometry.LineString(x))
		
		# encountering_pairs.loc[:, CFG_PRODUCER_KEY] = encountering_pairs.pair.apply(lambda l: str(l[0]))
		encountering_pairs.pair = encountering_pairs.pair.apply(lambda l: tuple(map(int, sorted(l))))
		encountering_pairs = encountering_pairs.groupby(
			['pair', 'dist', 'start', 'end'], 
			group_keys=False
		)['geometry'].apply(
			lambda l: shp.geometry.MultiLineString(l.values.tolist())
		).reset_index()

		encountering_pairs.loc[:, 'is_active'] = 'Y' if tag == 'active' else 'N'

	except Exception as e:
		# if it fails (for some reason) return an empty DataFrame
		print(f'\t\t----- Exception @ ```__fetch_latest_encounters``` ----- \nLog Traceback: {e}')
		encountering_pairs = gpd.GeoDataFrame(data=[], columns=['pair', 'dist', 'start', 'end', 'geometry', 'is_active'], geometry='geometry', crs=CFG_DATASET_CRS)
	finally:
		return gpd.GeoDataFrame(encountering_pairs, geometry='geometry', crs=CFG_DATASET_CRS)


def fetch_latest_encounters(consumer_enc):
	try:
		message_enc = consumer_enc.poll(max_records=1)
		message_enc = next(iter(message_enc.values()))[0]
		message_enc_data = eval(message_enc.value.decode('utf-8'))
	except StopIteration as si:
		print(f'\t\t----- Message Poll Failed ----- \nLog Traceback: {si}')
		message_enc_data = None

	encounter_active = __fetch_latest_encounters(message_enc_data, tag='active')
	encounter_active.end = ''
	encounter_closed = __fetch_latest_encounters(message_enc_data, tag='closed')

	result = pd.concat([encounter_active, encounter_closed], ignore_index=True)
	return result.sort_values('start', ascending=False).reset_index()


def fetch_latest_cri(consumer_vcra):
	try:
		message_cri = consumer_vcra.poll(max_records=1)
		message_cri = next(iter(message_cri.values()))[0]
	except StopIteration as si:
		print(f'\t\t----- Message Poll Failed ----- \nLog Traceback: {si}')
		message_cri = None
	
	try:
		message_cri_data = pd.read_json(message_cri.value.decode('utf-8'), orient='table').astype(
			{
				f'own_{CFG_PRODUCER_KEY}':int, 
				f'target_{CFG_PRODUCER_KEY}':int
			}
		)
		message_cri_data.own_geometry = message_cri_data.own_geometry.apply(wkt.loads)
		message_cri_data.index = message_cri_data.index.set_levels(
			pd.to_datetime(
				message_cri_data.index.levels[0], unit=CFG_PRODUCER_TIMESTAMP_UNIT
			), 
			level=0
		)
		message_cri_data = gpd.GeoDataFrame(message_cri_data, geometry='own_geometry', crs=CFG_DATASET_CRS)
	except Exception as e:
		# if it fails (for some reason) return an empty GeoDataFrame
		print(f'\t\t----- Exception @ ```fetch_latest_cri``` ----- \nLog Traceback: {e}')
		message_cri_data = gpd.GeoDataFrame(
			data=[], columns=[CFG_PRODUCER_TIMESTAMP_NAME, *CFG_CRI_DATASET_FEATURES], geometry='own_geometry', crs=CFG_DATASET_CRS
		).set_index(['timestamp'], append=True).reorder_levels((1,0))

	return message_cri_data


def get_data(consumer_vcra, consumer_enc, consumer_op):
	cri_data = fetch_latest_cri(consumer_vcra)
	encounters = fetch_latest_encounters(consumer_enc)	
	ais_points, curr_timestamp = fetch_latest_positions(consumer_op)

	## Meteorological Discipline
	ais_points.loc[:, 'DSCMP'] = DEG2GEO(ais_points[CFG_CONSUMER_COURSE_NAME])
	ais_points.loc[:, 'TRCMP'] = 240 - ais_points[CFG_CONSUMER_COURSE_NAME]

	ais_points_mi = ais_points.rename_axis([CFG_PRODUCER_TIMESTAMP_NAME, 'own_index'])
	cri_data_mi = cri_data.reset_index(1, drop=True).set_index(['own_index'], append=True)\
					 .reorder_levels((0,1))[[f'own_{CFG_PRODUCER_KEY}', 'ves_cri']]\
					 .groupby([CFG_PRODUCER_TIMESTAMP_NAME, 'own_index', f'own_{CFG_PRODUCER_KEY}'], group_keys=False)\
					 .apply(lambda l: l.loc[l.ves_cri == l.ves_cri.max()])\
					 .ves_cri

	if cri_data_mi.empty:
		cri_data_mi = cri_data_mi.reset_index(level=-1)

	## Add CRI to current/past encounters
	if encounters.empty or cri_data.empty:
		encounters[['cri_own', 'cri_target']] = np.nan
	else:
		encounters[['cri_own', 'cri_target']] = pd.DataFrame(
			encounters.apply(
				lambda l: pd.Series(
					cri_data.loc[pd.IndexSlice[l.start:l.end]].query(
						f'own_{CFG_PRODUCER_KEY} in {l.pair} & target_{CFG_PRODUCER_KEY} in {l.pair}'
					).groupby(
						[f'own_{CFG_PRODUCER_KEY}', f'target_{CFG_PRODUCER_KEY}']
					).ves_cri.mean().values),
				axis=1
			).values.tolist()
		)

	## TODO: Replace with actual CRIs -- DONE
	ais_points = ais_points_mi.join(
		cri_data_mi,
		how='left',
		on=[CFG_PRODUCER_TIMESTAMP_NAME, 'own_index']
	).reset_index(0)

	ais_points.sort_values('timestamp', ascending=False, inplace=True)

	return encounters, cri_data, ais_points, curr_timestamp


def toggle_renderers(viz):
	for renderer in viz.renderers: 
		renderer.visible = not renderer.visible


def render_items(df, st_viz_instance):
	st_viz_instance.canvas_data = df.to_crs(epsg=3857)
	st_viz_instance.canvas_data = st_viz_instance.prepare_data(st_viz_instance.canvas_data)
	st_viz_instance.source.data = st_viz_instance.canvas_data.drop(st_viz_instance.canvas_data.geometry.name, axis=1).to_dict(orient="list")


def track_selected(df, column, vals):
	try:
		ix = df.loc[df[column].isin(vals)].index.values.tolist()
		return ix
	except ValueError as e:
		print(f'\t\t----- Exception @ ```track_selected``` ----- \nLog Traceback: {e}')
		return df.index.values.tolist()
