import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import os

import pandas as pd
import dash_table
import uuid
from dash.exceptions import PreventUpdate
from dash_extensions import Download

import math
import io
from dash_table.Format import Format

from flask import current_app, session

assets_path = current_app.config['CSS_PATH']

external_stylesheets = [
    'https://codepen.io/chriddyp/pen/bWLwgP.css', 
    dbc.themes.BOOTSTRAP
]

APP_ID = 'dashboardapp'
URL_BASE = '/dashboardapp'
MIN_HEIGHT = 200
PAGE_SIZE = 20


def fetch_details(name):
    if name == 'APP_ID':
        return APP_ID+str(uuid.uuid4())
    else:
        return URL_BASE + str(uuid.uuid4()) + '/'
    

def add_dash(name, server, URL_BASE):    
    dash_app = dash.Dash(
        name=name,
        server=server,
        assets_folder=assets_path,
        url_base_pathname=URL_BASE,
        suppress_callback_exceptions=False, 
        external_stylesheets=external_stylesheets, 
        serve_locally=True,
        meta_tags=[{'name': 'viewport',
                    'content': 'width=device-width, initial-scale=1.0, maximum-scale=1.2, minimum-scale=0.5,'}]
    )
    return dash_app


def ui_layout(app, df):

    from scpat.dashapp.dash_ui_comp import generate_layout, register_callback

    app.layout = generate_layout(df)

    register_callback(app, df)
        
