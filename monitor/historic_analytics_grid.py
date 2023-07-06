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


SPATIAL_GRID_SIZE = 10


def process_encountering_processes(encountering_pairs, vcra_results):
    # Add CRI Information
    if encountering_pairs.empty or vcra_results.empty:
        encountering_pairs['ves_cri'] = np.nan
    else:
        encountering_pairs.loc[:, 'ves_cri'] = encountering_pairs.apply(
            lambda l: vcra_results.loc[
                pd.IndexSlice[l.start:l.end, :]
            ].loc[
                (vcra_results[f'own_{CFG_PRODUCER_KEY}'] == l.pair[0]) & 
                (vcra_results[f'target_{CFG_PRODUCER_KEY}'] == l.pair[1])
            ].ves_cri.mean(), 
            axis=1
        )
    
    # Parse Geometry, and Start/End timestamps 
    if not encountering_pairs.empty:
        emerged_encounters = encountering_pairs.geometry.apply(len) < 2
        encountering_pairs = encountering_pairs.loc[~emerged_encounters].copy()

    encountering_pairs.geometry = encountering_pairs.geometry.apply(lambda x: shp.geometry.LineString(x))
    encountering_pairs.start = pd.to_datetime(encountering_pairs.start, unit=CFG_PRODUCER_TIMESTAMP_UNIT)
    encountering_pairs.end = pd.to_datetime(encountering_pairs.end, unit=CFG_PRODUCER_TIMESTAMP_UNIT)

    return gpd.GeoDataFrame(encountering_pairs, geometry='geometry', crs=CFG_DATASET_CRS)


def generate_spatial_grid(bbox, crs_from=4326, cells_no=10):
    spatial_coverage = gpd.GeoSeries(shp.geometry.box(*bbox), index=['geom'], crs=crs_from).to_crs(3857)
    
    # Calculate cutoff value, according to ```cells_no```
    cutoff_pnt = np.around(
        np.mean([
            np.diff(spatial_coverage.total_bounds[[0, -2]]) / cells_no,\
            np.diff(spatial_coverage.total_bounds[[1, -1]]) / cells_no
        ]),
    )
    
    # Generate Spatial Grid
    return gpd.GeoDataFrame(
        *viz_helper.quadrat_cut_geometry(spatial_coverage, cutoff_pnt), 
        columns=['geometry'], 
        geometry='geometry', 
        crs=3857
    )


def mean_cri_per_cell(spatial_grid, geoms, crs_to=3857):
    sindex = spatial_grid.sindex
    
    ix, area_id = sindex.query_bulk(geoms.to_crs(crs_to).geometry, predicate='intersects', sort=True)
    geoms.loc[ix, 'area_id'] = area_id
    
    spatial_grid.loc[:, 'mean_cri'] = pd.merge(
        spatial_grid.reset_index(), 
        geoms[['area_id', 'ves_cri']], 
        left_on='index', right_on='area_id', 
        how='left'
    ).groupby('index').ves_cri.mean()
    
    return spatial_grid


def build_hist_spatial_grid():
    '''Create ST_Visions instances'''
    # x_range, y_range, sizing_mode, plot_width, plot_height = (-590039, -187796), (5764490, 6158796), 'stretch_both', 250, 250
    x_range, y_range, sizing_mode, plot_width, plot_height = (CFG_DATASET_MBB[0], CFG_DATASET_MBB[2]), (CFG_DATASET_MBB[1], CFG_DATASET_MBB[3]), 'stretch_both', 250, 250

    # Vessel Encounters
    df_enc_hist = gpd.GeoDataFrame(
        data=[], columns=['pair', 'dist', 'start', 'end', 'geometry', 'ves_cri'], geometry='geometry', crs=CFG_DATASET_CRS
    )
    # CRI Results
    df_vcra_hist = gpd.GeoDataFrame(
        data=[], columns=[CFG_PRODUCER_TIMESTAMP_NAME, *CFG_CRI_DATASET_FEATURES], geometry='own_geometry', crs=CFG_DATASET_CRS
    ).set_index(['timestamp'], append=True).reorder_levels((1,0))


    dataset = process_encountering_processes(df_enc_hist.copy(), df_vcra_hist.copy())
    spatial_coverage_cut = generate_spatial_grid(CFG_DATASET_MBB, crs_from=3857)
    spatial_coverage_cut = mean_cri_per_cell(spatial_coverage_cut.copy(), dataset.copy())
    

    st_viz = st_visualizer(limit=int(1e4))
    st_viz.set_data(spatial_coverage_cut.copy())

    st_viz.create_canvas(
        x_range=x_range, y_range=y_range, 
        title=f'Monitoring high-risk (in terms of CRI) Areas of Interest', 
        sizing_mode=sizing_mode, plot_width=plot_width, plot_height=plot_height,
        height_policy='max', tools="pan,box_zoom,lasso_select,wheel_zoom,save,reset", output_backend='webgl')
    st_viz.add_map_tile('CARTODBPOSITRON')

    # Visualize Spatial Grid
    st_viz.add_numerical_colormap(
        bokeh_palettes.Spectral5, 'mean_cri', val_range=[0,1], nan_color='rgba(0, 0, 0, 0)', colorbar=True, 
        cb_orientation='vertical', cb_location='right', 
        label_standoff=12, border_line_color=None, location=(0,0), 
        title='CRI Risk Levels (low; low-middle; middle; middle-high; high)'
    )

    poly_grid = st_viz.add_polygon(fill_color=st_viz.cmap, line_color=st_viz.cmap, fill_alpha=0.6, muted_alpha=0)
    st_viz.add_hover_tooltips(tooltips=[('Avg. CRI','@mean_cri{0.0}')], mode="mouse", muted_policy='ignore', renderers=[poly_grid,])


    # Adjust Spatial Grid (and average CRI) on Pan/Zoom
    def classify_proximity(bbox):
        global SPATIAL_GRID_SIZE
        updated_spatial_grid = generate_spatial_grid(bbox, crs_from=3857, cells_no=SPATIAL_GRID_SIZE)
        
        updated_spatial_grid = mean_cri_per_cell(
            updated_spatial_grid.copy(), 
            st_viz2.data.loc[st_viz2.source.data['index']].copy().reset_index()
        )
        render_items(updated_spatial_grid, st_viz)
        
    st_viz.figure.on_event(bokeh.events.RangesUpdate, lambda bbox: classify_proximity([bbox.x0, bbox.y0, bbox.x1, bbox.y1]))


    # Add CRI Alert Threshold
    class cri_filter_cb(viz_cbs.BokehFilters):
        def __init__(self, vsn_instance, widget):
            super().__init__(vsn_instance, widget)

        def callback(self, attr, old, new):
            self.vsn_instance.cmap['transform'].low = new
            
    cri_filter = bokeh_models.Slider(start=0, end=1, step=0.1, value=0, title='CRI Threshold', height_policy='min', width_policy='min')
    cri_filter.on_change('value', cri_filter_cb(st_viz, cri_filter).callback)
    st_viz.widgets.append(cri_filter)


    # Add Spatial Resolution Slider
    class res_filter_cb(viz_cbs.BokehFilters):
        def __init__(self, vsn_instance, widget):
            self.widget = widget
            super().__init__(vsn_instance, widget)

        def callback(self, attr, old, new):
            global SPATIAL_GRID_SIZE
            SPATIAL_GRID_SIZE, bbox = new, [
                self.vsn_instance.figure.x_range.start, self.vsn_instance.figure.y_range.start, 
                self.vsn_instance.figure.x_range.end, self.vsn_instance.figure.y_range.end
            ]
            classify_proximity(bbox)
            
    res_filter = bokeh_models.Slider(start=SPATIAL_GRID_SIZE, end=50, step=SPATIAL_GRID_SIZE, value=SPATIAL_GRID_SIZE, title='Spatial Grid Resolution', height_policy='min', width_policy='min')
    res_filter.on_change('value_throttled', res_filter_cb(st_viz, res_filter).callback)
    st_viz.widgets.append(res_filter)


    # [OPTIONAL] PLOT ENCOUNTERING GEOMETRIES
    st_viz2 = st_visualizer(limit=int(1e4))
    st_viz2.set_data(dataset.reset_index().copy())

    st_viz2.set_figure(st_viz.figure)
    st_viz2.create_source()

    _ = st_viz2.add_line(line_color='mintcream', line_width=5, alpha=0.5, muted_alpha=0, nonselection_alpha=0)

    # Add temporal filter...
    temp_filter = st_viz2.add_temporal_filter(
        temporal_name='start', 
        start_date=pd.to_datetime(0, unit=CFG_PRODUCER_TIMESTAMP_UNIT), 
        end_date=pd.to_datetime(1, unit=CFG_PRODUCER_TIMESTAMP_UNIT), 
        temporal_unit=None, step_ms=1800000, height_policy='min', width_policy='min'
    )
    # ...Link filter with ```st_viz``` instance (trigger callback sync)...
    temp_filter.on_change('value_throttled', lambda attr, old, new: res_filter.trigger('value_throttled', None, res_filter.value))

    st_viz2.figure.toolbar.active_scroll = st_viz.figure.select_one(bokeh_models.WheelZoomTool)

    return st_viz2.prepare_grid(
        [
            [
                bokeh_layouts.row(
                    [res_filter, bokeh_models.Div(), cri_filter, bokeh_models.Div(), temp_filter, bokeh_models.Div()], 
                    height_policy='min', width_policy='max'
                )
            ], [
                st_viz2.figure
            ]
        ],
        toolbar_options=dict(logo=None), 
        sizing_mode='stretch_height', 
        toolbar_location='right'
    ), [
        st_viz, 
        st_viz2
    ]
