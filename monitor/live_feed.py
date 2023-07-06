import bokeh
import bokeh.models as bokeh_models
import bokeh.layouts as bokeh_layouts
import bokeh.palettes as bokeh_palettes

import sys, os
sys.path.append(os.path.join(os.path.expanduser('~'), 'Documents'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lib'))

import geopandas as gpd
from kafka_config_c_p_v01 import *

import st_visions.callbacks as viz_cbs
from st_visions.st_visualizer import st_visualizer


def build_live_feed():
    '''Create ST_Visions instances'''
    # x_range, y_range, sizing_mode, plot_width, plot_height = (-556597, -467542), (6106855, 6190444), 'stretch_both', 250, 250
    x_range, y_range, sizing_mode, plot_width, plot_height = (CFG_DATASET_MBB[0], CFG_DATASET_MBB[2]), (CFG_DATASET_MBB[1], CFG_DATASET_MBB[3]), 'stretch_both', 250, 250

    datefmt_str, coords_str, = "%Y-%m-%d %H:%M:%S", "0.0000"
    datefmt, coords_hfmt = bokeh.models.DateFormatter(format=datefmt_str), bokeh.models.NumberFormatter(format=coords_str)
    distance_hfmt, num_hfmt = bokeh.models.NumberFormatter(format="0"), bokeh.models.NumberFormatter(format="0.0")

    # Design the First view of the Platform (prior to the Update)
    # df_anchs      = gpd.GeoDataFrame(data=[], columns=['clusters', 'st', 'et', 'geometry', 'status', 'type'], geometry='geometry', crs=CFG_DATASET_CRS)
    df_points     = gpd.GeoDataFrame(data=[], columns=[*CFG_BUFFER_COLUMN_NAMES, 'is_moving', 'geometry', 'DSCMP', 'TRCMP', 'ves_cri'], geometry='geometry', crs=CFG_DATASET_CRS)
    df_encounters = gpd.GeoDataFrame(data=[], columns=['pair', 'dist', 'start', 'end', 'geometry', 'is_active', CFG_PRODUCER_KEY], geometry='geometry', crs=CFG_DATASET_CRS)


    # Instance #1 - Vessels' (AIS) Points
    st_viz = st_visualizer(limit=30000)
    st_viz.set_data(df_points.copy())

    basic_tools = "tap,pan,wheel_zoom,save,reset" 

    st_viz.create_canvas(x_range=x_range, y_range=y_range, 
                        #  title=f'Monitoring Vessel Encounters @{datetime.datetime.now(datetime.timezone.utc).strftime("%d-%m-%Y %H:%M:%S")} UTC',
                        title=f'Monitoring Vessel Encounters (Loading...)',
                        sizing_mode=sizing_mode, plot_width=plot_width, plot_height=plot_height, 
                        height_policy='max', tools=basic_tools, output_backend='webgl')
    st_viz.add_map_tile('CARTODBPOSITRON')

    # Add (Different) Colors for Active and Closed Stationary ACs
    ais_moving = bokeh_models.CDSView(source=st_viz.source, filters=[bokeh_models.GroupFilter(column_name='is_moving', group='Y')])
    ais_stat = bokeh_models.CDSView(source=st_viz.source, filters=[bokeh_models.GroupFilter(column_name='is_moving', group='N')])

    st_viz.add_numerical_colormap(
        bokeh_palettes.Spectral5, 'ves_cri', val_range=[0,1], nan_color='#3288bd', colorbar=True, 
        cb_orientation='horizontal', cb_location='below', 
        label_standoff=12, border_line_color=None, location=(0,0), 
        title='CRI Risk Levels (low; low-middle; middle; middle-high; high)'
    )

    # Circle (dot) for Stationary Vessels
    ais_pts_stat = st_viz.add_glyph(
        glyph_type='triangle', fill_color=st_viz.cmap, line_color=st_viz.cmap, size=7, 
        alpha=0.85, fill_alpha=0.85, muted_alpha=0, nonselection_alpha=0, 
        legend_label=f'AIS Points (Stationary)', view=ais_stat
    )
    # Arrow for Moving Vessels
    ais_pts_moving = st_viz.add_glyph(
        glyph_type='circle', fill_color=st_viz.cmap, line_color=st_viz.cmap, size=7, 
        alpha=0.85, fill_alpha=0.85, muted_alpha=0, nonselection_alpha=0, 
        legend_label=f'AIS Points (Moving)', view=ais_moving
    )

    tooltips = [
        ('Vessel ID','@mmsi'), ('Timestamp','@timestamp{{{0}}} UTC'.format(datefmt_str)), 
        ('Location (lon., lat.)','(@lon{{{0}}}, @lat{{{0}}})'.format(coords_str)), 
        ('Speed (knots)', '@speed{0.0}'), ('Course (deg.)', '@course{0.0}'), 
        ('Length (m.)', '@length{0.0}'), ('Moving (Y/N)', '@is_moving'), ('CRI (%)', '@ves_cri{0.0}')
    ]
    st_viz.add_hover_tooltips(
        tooltips=tooltips, formatters={'@timestamp':'datetime'},
        mode="mouse", muted_policy='ignore', 
        renderers=[ais_pts_moving, ais_pts_stat]
    )

    columns_ais = [
        bokeh_models.TableColumn(field="mmsi", title="MMSI", sortable=False),
        bokeh_models.TableColumn(field="timestamp", title="Timestamp (UTC)", default_sort='descending', formatter=datefmt),
        bokeh_models.TableColumn(field="lon", title="Longitude (deg.)", sortable=False, formatter=coords_hfmt),
        bokeh_models.TableColumn(field="lat", title="Latitude (deg.)", sortable=False, formatter=coords_hfmt),
        bokeh_models.TableColumn(field="speed", title="Speed (knots)", sortable=False, formatter=num_hfmt),
        bokeh_models.TableColumn(field="course", title="Course (deg.)", sortable=False, formatter=num_hfmt),
        bokeh_models.TableColumn(field="length", title="Length (m)", sortable=False, formatter=distance_hfmt),
        bokeh_models.TableColumn(field="ves_cri", title="CRI (%)", sortable=False, formatter=num_hfmt)
        # bokeh_models.TableColumn(field="is_moving", title="Moving (Y/N)", sortable=False)
    ]
    data_table_ais_title = bokeh_models.Div(text=f'''<h4>Vessels' Points</h4>''', height_policy='min')    # Margin-Top, Margin-Right, Margin-Bottom and Margin-Left, similar to CSS standards.
    data_table_ais = bokeh_models.DataTable(
        source=st_viz.source, columns=columns_ais, 
        height_policy='max', width_policy='max', 
        height=plot_height, width=plot_width
    )	

    # Add CRI alert threshold
    cri_filter = bokeh_models.Slider(start=0, end=1, step=0.1, value=0, title='CRI Threshold', height_policy='min')

    class cri_filter_cb(viz_cbs.BokehFilters):
        def __init__(self, vsn_instance, widget):
            super().__init__(vsn_instance, widget)
        
        def callback(self, attr, old, new):
            print(attr, old, new)
            self.vsn_instance.cmap['transform'].low = new
    
    cri_filter.on_change('value', cri_filter_cb(st_viz, cri_filter).callback)
    st_viz.widgets.append(cri_filter)

    # Set Active Toolkits
    st_viz.figure.toolbar.active_scroll = st_viz.figure.select_one(bokeh_models.WheelZoomTool)
    st_viz.figure.toolbar.active_tap = st_viz.figure.select_one(bokeh_models.TapTool)


    # Instance #2 - Vessels Encounters
    st_viz2 = st_visualizer(limit=30000)
    st_viz2.set_data(df_encounters.copy())
    
    st_viz2.set_figure(st_viz.figure)
    st_viz2.create_source()

    # Add (Different) Colors for Active and Inactive Encounters
    enc_moving = bokeh_models.CDSView(source=st_viz2.source, filters=[bokeh_models.GroupFilter(column_name='is_active', group='Y')])
    enc_stat = bokeh_models.CDSView(source=st_viz2.source, filters=[bokeh_models.GroupFilter(column_name='is_active', group='N')])

    enc_moving_line = st_viz2.add_line(legend_label='Active Encounters', line_color='darkmagenta', line_width=5, muted_alpha=0, nonselection_alpha=0, view=enc_moving)
    enc_stat_line = st_viz2.add_line(legend_label='Inactive Encounters', line_color='darkslategray', line_width=5, alpha=0.5, muted_alpha=0, nonselection_alpha=0, view=enc_stat)

    tooltips = [('Encountering Vessels','@pair'), ('Cluster Start','@start{{{0}}}'.format(datefmt_str)), ('Cluster End','@end{{{0}}}'.format(datefmt_str))]
    st_viz2.add_hover_tooltips(tooltips=tooltips, formatters={'@start':'datetime', '@end':'datetime'}, mode="mouse", muted_policy='ignore', renderers=[enc_moving_line, enc_stat_line])

    columns_ec = [
        bokeh_models.TableColumn(field="pair", title="Enountering Vessels", sortable=False),
        bokeh_models.TableColumn(field="dist", title="Distance (m)", sortable=False, formatter=distance_hfmt),
        bokeh_models.TableColumn(field="start", title="Start", default_sort='descending', formatter=datefmt),
        bokeh_models.TableColumn(field="end", title="End", sortable=False, formatter=datefmt),
        bokeh_models.TableColumn(field="cri_own", title="CRI (%; Own)", sortable=False, formatter=num_hfmt),
        bokeh_models.TableColumn(field="cri_target", title="CRI (%; Target)", sortable=False, formatter=num_hfmt),
        bokeh_models.TableColumn(field="is_active", title="Active (Y/N)", sortable=False)
    ]
    data_table_enc_title = bokeh_models.Div(text=f'''<h4>Encountering Processes</h4>''', height_policy='min')    # Margin-Top, Margin-Right, Margin-Bottom and Margin-Left, similar to CSS standards.
    data_table_enc = bokeh_models.DataTable(source=st_viz2.source, columns=columns_ec, height_policy='max', width_policy='max', height=plot_height, width=plot_width)

    return st_viz2.prepare_grid(
        [[st_viz2.figure, bokeh_layouts.column([cri_filter, data_table_enc_title, data_table_enc, data_table_ais_title, data_table_ais])]], 
        toolbar_options=dict(logo=None), 
        sizing_mode='stretch_both', 
        toolbar_location='right'
    ), [
        st_viz, 
        st_viz2
    ]
