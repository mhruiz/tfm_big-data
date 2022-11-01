import dash
import pandas as pd
import plotly.express as px

from dash import dcc, html

from dash.dependencies import Input, Output

import numpy as np

import datetime

# https://community.plotly.com/t/how-to-get-real-time-data-updating-on-a-scatter-mapbox/48807

app = dash.Dash()

df = pd.read_csv('data.csv')

fig = px.scatter_mapbox(df, lat="lat", lon="lon", zoom=11, color='percentage', color_continuous_scale=["green", 'yellow', "red"])
fig.update_layout(mapbox_style="open-street-map")

app.layout = html.Div(children=[
    html.H1(children='Contaminación acústica Madrid'),

    html.Div(id='last-update'),

    # https://stackoverflow.com/questions/46287189/how-can-i-change-the-size-of-my-dash-graph
    dcc.Graph(
        id='live-update-graph',
        style={'width': '90vw', 'height': '90vh'},
        figure=fig
    ),

    dcc.Interval(
        id='interval-component', 
        interval=1*500, 
        n_intervals=0
    )
])

@app.callback(Output('last-update','children'), Input('interval-component', 'n_intervals'))
def update_graph_live(n):
    # udf = pd.read_csv('data.csv')
    # fig.update(data=dict(lat=np.array(udf.lat).tolist(), lon=np.array(udf.lon).tolist()))
    # return fig
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@app.callback(
    Output('live-update-graph', 'figure'),
    # [Input('refresh-data','n_clicks')])
    Input('interval-component', 'n_intervals'))
def refresh_data(value):

    df = pd.read_csv('data.csv')

    fig = px.scatter_mapbox(df, lat="lat", lon="lon", zoom=11, color='percentage', color_continuous_scale=["green", 'yellow', "red"])
    fig.update_layout(mapbox_style="open-street-map")

    return fig


if __name__ == '__main__':
    app.run_server(debug=True, use_reloader=False)