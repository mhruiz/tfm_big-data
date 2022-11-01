import dash
import pandas as pd
import plotly.express as px

import plotly.graph_objects as go

from dash import dcc, html

from dash.dependencies import Input, Output

import numpy as np

import datetime


from lib.data import constants, raw
import lib

# https://community.plotly.com/t/how-to-get-real-time-data-updating-on-a-scatter-mapbox/48807

app = dash.Dash()

noise_data, last_sync = lib.app.update_data()


def load_fig(noise_data):
    
    print('Cargando nuevos noise_data')

    fig = px.scatter_mapbox(noise_data, 
                            lat="latitud", 
                            lon="longitud", 
                            zoom=11, 
                            color='Ruido medio (dB)',  
                            range_color=[30, 130], 
                            color_continuous_scale=["green", 'yellow', "red"])
    fig.update_layout(mapbox_style="open-street-map")
    return fig

fig = load_fig(noise_data)

app.layout = html.Div(children=[
    html.H1(children='Contaminación acústica Madrid'),

    html.Div(id='current-time'),

    html.Div(id='next-sync'),

    # https://stackoverflow.com/questions/46287189/how-can-i-change-the-size-of-my-dash-graph
    dcc.Graph(
        id='live-update-graph',
        style={'width': '90vw', 'height': '90vh'},
        figure=fig
    ),

    dcc.Interval(
        id='interval-clock', 
        interval=1*100, 
        n_intervals=0
    ),

    dcc.Interval(
        id='interval-component', 
        interval=1*1000, 
        n_intervals=0
    )
])

@app.callback(Output('current-time','children'), Input('interval-clock', 'n_intervals'))
def update_clock_live(n):
    return f'Hora actual: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'


@app.callback(Output('next-sync','children'), Input('interval-clock', 'n_intervals'))
def update_next_sync(n):
    global last_sync
    return f'Hora de los datos: {last_sync.strftime("%Y-%m-%d %H:%M:%S")}'


@app.callback(
    Output('live-update-graph', 'figure'),
    Input('interval-component', 'n_intervals'))
def refresh_data(value):

    global noise_data
    global last_sync
    global fig

    noise_data, data_sync = lib.app.update_data(last_sync)

    if last_sync == data_sync:
        return fig

    print('Update datetime:', data_sync)
    last_sync = data_sync

    return load_fig(noise_data)

if __name__ == '__main__':
    app.run(debug=True)