import bokeh
import bokeh.models as bokeh_models
import bokeh.layouts as bokeh_layouts
import bokeh.palettes as bokeh_palettes

import sys, os
sys.path.append(os.path.join(os.path.expanduser('~'), 'Documents'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lib'))

import numpy as np
import shapely as shp

import pandas as pd
import geopandas as gpd

from kafka_config_c_p_v01 import *
from monitor.vessel_positions_json import render_items

import st_visions.callbacks as viz_cbs
import st_visions.geom_helper as viz_helper 
from st_visions.st_visualizer import st_visualizer



def build_hist_vessel_trajs():
    '''Create ST_Visions instances'''
    # x_range, y_range, sizing_mode, plot_width, plot_height = (-590039, -187796), (5764490, 6158796), 'stretch_both', 250, 250
    x_range, y_range, sizing_mode, plot_width, plot_height = (CFG_DATASET_MBB[0], CFG_DATASET_MBB[2]), (CFG_DATASET_MBB[1], CFG_DATASET_MBB[3]), 'stretch_both', 250, 250


    datefmt_str, coords_str, = "%Y-%m-%d %H:%M:%S", "0.0000"
    datefmt, coords_hfmt = bokeh.models.DateFormatter(format=datefmt_str), bokeh.models.NumberFormatter(format=coords_str)
    distance_hfmt, num_hfmt = bokeh.models.NumberFormatter(format="0"), bokeh.models.NumberFormatter(format="0.0")

    dataset = gpd.GeoDataFrame(
        data=[], columns=[
            CFG_PRODUCER_TIMESTAMP_NAME, CFG_PRODUCER_KEY, *CFG_CONSUMER_COORDINATE_NAMES, 
            CFG_CONSUMER_SPEED_NAME, CFG_CONSUMER_COURSE_NAME, CFG_CONSUMER_LENGTH_NAME, 
            'target_mmsi', 'ves_cri', 'geometry'
        ], geometry='geometry', crs=CFG_DATASET_CRS
    )

    # Create ST_Visions Instance
    st_viz = st_visualizer(limit=int(3e4))
    st_viz.set_data(dataset.astype({CFG_PRODUCER_KEY:str}).copy())

    st_viz.create_canvas(
        x_range=x_range, y_range=y_range, 
        title=f'Monitoring the CRI of a Specific Vessel', 
        sizing_mode=sizing_mode, plot_width=plot_width, plot_height=plot_height,
        height_policy='max', tools="pan,box_zoom,lasso_select,wheel_zoom,save,reset", output_backend='webgl')
    st_viz.add_map_tile('CARTODBPOSITRON')

    st_viz.add_numerical_colormap(
        bokeh_palettes.Spectral5, 'ves_cri', val_range=[0,1], nan_color='#3288bd', colorbar=True, 
        cb_orientation='vertical', cb_location='right', 
        label_standoff=12, border_line_color=None, location=(0,0), 
        title='CRI Risk Levels (low; low-middle; middle; middle-high; high)'
    )

    vessel_id_filter = st_viz.add_categorical_filter(
        title='Select Vessel', categorical_name=CFG_PRODUCER_KEY, height_policy='min', width_policy='min'
    )

    pts_time_filter = st_viz.add_temporal_filter(
        title='Temporal Horizon', 
        temporal_name=CFG_PRODUCER_TIMESTAMP_NAME, 
        start_date=pd.to_datetime(0, unit=CFG_PRODUCER_TIMESTAMP_UNIT), 
        end_date=pd.to_datetime(1, unit=CFG_PRODUCER_TIMESTAMP_UNIT), 
        temporal_unit=CFG_PRODUCER_TIMESTAMP_UNIT, step_ms=600000, height_policy='min', width_policy='min', callback_policy='value'
    )

    ais_pts = st_viz.add_glyph(color=st_viz.cmap, alpha=0.7, muted_alpha=0)
    tooltips = [
        ('Vessel ID; Own','@mmsi'), ('Timestamp','@{1}{{{0}}} UTC'.format(datefmt_str, CFG_PRODUCER_TIMESTAMP_NAME)), 
        ('Location (lon., lat.)','(@lon, @lat)'), 
        ('Speed (knots)', '@speed{0.0}'), ('Course (deg.)', '@course{0.0}'), 
        ('Length (m.)', '@length'),
        ('CRI (%)', '@ves_cri{0.0}'),
        ('Vessel ID; Target', '@target_mmsi')
    ]
    st_viz.add_hover_tooltips(
        tooltips=tooltips, formatters={'@timestamp':'datetime'},
        mode="mouse", muted_policy='ignore', 
        renderers=[ais_pts]
    )

    # Add CRI alert threshold
    cri_filter = bokeh_models.Slider(start=0, end=1, step=0.1, value=0, title='CRI Threshold', height_policy='min', width_policy='min')

    class cri_filter_cb(viz_cbs.BokehFilters):
        def __init__(self, vsn_instance, widget):
            super().__init__(vsn_instance, widget)

        def callback(self, attr, old, new):
            # print(attr, old, new)  
            self.vsn_instance.cmap['transform'].low = new

    cri_filter.on_change('value', cri_filter_cb(st_viz, cri_filter).callback)
    st_viz.widgets.append(cri_filter)

    # Set Active Toolkits
    st_viz.figure.toolbar.active_scroll = st_viz.figure.select_one(bokeh_models.WheelZoomTool)
    st_viz.figure.toolbar.active_tap = st_viz.figure.select_one(bokeh_models.TapTool)

    return st_viz.prepare_grid(
        [
            [
                bokeh_layouts.row(
                    [vessel_id_filter, bokeh_models.Div(), cri_filter, bokeh_models.Div(), pts_time_filter, bokeh_models.Div()], 
                    height_policy='min', width_policy='max'
                )
            ], [
                st_viz.figure
            ]
        ],
        toolbar_options=dict(logo=None), 
        sizing_mode='stretch_height', 
        toolbar_location='right'
    ), [
        st_viz, 
    ]
