import dash
from dash.dependencies import Input, Output, State
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


from flask import current_app

assets_path = current_app.config['CSS_PATH']

external_stylesheets = [
    'https://codepen.io/chriddyp/pen/bWLwgP.css', 
    dbc.themes.BOOTSTRAP
]

APP_ID = 'generatesummaryapp'
URL_BASE = '/generate_summary'
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
        suppress_callback_exceptions=True, 
        external_stylesheets=external_stylesheets, 
        meta_tags=[{'name': 'viewport',
                    'content': 'width=device-width, initial-scale=1.0, maximum-scale=1.2, minimum-scale=0.5,'}]
    )
    return dash_app

def register_callback(app, df):  
 
    @app.callback(
        [Output('table-paging-with-graph', 'data'), 
        Output('table-paging-with-graph', 'page_count')],   
        [Input('table-paging-with-graph-dropdown-month', 'value'),     
        Input('table-paging-with-graph-dropdown-product-code', 'value'),
        Input('table-paging-with-graph-dropdown-mother-div', 'value'),
        Input('table-paging-with-graph-dropdown-sales-div', 'value'),
        Input('table-paging-with-graph-dropdown-material-type', 'value'),
        Input('table-paging-with-graph', "page_current"),
        Input('table-paging-with-graph', "page_size"),
        Input('table-paging-with-graph', "sort_by")   ])
    def update_filteredtable(input_month, input_prodcode, input_motherdiv, input_salesdiv, input_materialtype, page_current,page_size, sort_by):
        
        input_motherdiv = list(filter(None, input_motherdiv))
        input_salesdiv = list(filter(None, input_salesdiv))
        input_prodcode = list(filter(None, input_prodcode))
        input_materialtype = list(filter(None, input_materialtype))
        input_month = list(filter(None, input_month))

        if len(input_month) == 0:
            input_month = df['Month']
        if len(input_prodcode) == 0:
            input_prodcode = df['Product Code']
        if len(input_motherdiv) == 0:
            input_motherdiv = df['Sales Division']
        if len(input_materialtype) == 0:
            input_materialtype = df['Material Type']
        if len(input_salesdiv) == 0:
            input_salesdiv = df['Division']

        df_filtered = df[ (df['Month'].isin(input_month)) & (df['Product Code'].isin(input_prodcode)) & (df['Sales Division'].isin(input_motherdiv)) & (df['Division'].isin(input_salesdiv)) & (df['Material Type'].isin(input_materialtype))]

        if len(sort_by):
            dff= df_filtered.sort_values(
                    [col['column_id'] for col in sort_by],
                    ascending=[
                        col['direction'] == 'asc'
                        for col in sort_by
                    ],
                    inplace=False
                )
        else:
            dff = df_filtered

        page_count = math.ceil(len(dff)/page_size)
        if page_count == 0:
            page_count = 1
        
        return dff.iloc[page_current*page_size:(page_current+ 1)*page_size ].to_dict('records'), page_count

    @app.callback(
        Output("download", "data"),
        Input("save-button", "n_clicks"),
        State("table-paging-with-graph", "data"))
    def download_as_csv(n_clicks, table_data):
        dff = pd.DataFrame.from_dict({'Unique Id': df['Unique Id'], 'Date': df['Date'], 'Year': df['Year'], 'Month': df['Month'], 'Product Code': df['Product Code'], 
                'Product Description': df['Product Description'], 'Mother Division': df['Sales Division'], 'Sales Division': df['Division'], 'Material Type': df['Material Type'], 'Statistical Forecast ' : df[ 'Model Forecast' ],'Actual Forecast'    :  df[ 'Actual Forecast'],'Forecast KG    '    :  df[ 'Forecast KG'    ],'Actual Demand  '    :  df[ 'Actual Demand'  ],'Demand KG		'	: df[ 'Demand KG'      ] })
        if not n_clicks:
            raise PreventUpdate

        download_buffer = io.StringIO()
        dff.to_csv(download_buffer, index=False)
        download_buffer.seek(0)
        
        import datetime

        file= "Summary file "+datetime.datetime.today().strftime("%B %Y")+".csv"
        return dict(content=download_buffer.getvalue(), filename=file)


def ui_layout(app, df):

    dropdowncard1= dbc.Col([
                            dbc.Card(
                                    [
                                        dbc.CardBody(
                                            [
                                                dbc.Row([
                                                    dbc.Col(
                                                        dcc.Dropdown(
                                                                    id='table-paging-with-graph-dropdown-month',
                                                                    
                                                                    
                                                                    children=[
                                                                        dcc.Checklist(
                                                                        options=[
                                                                            {'label': i, 'value': i} for i in df['Month'].unique()
                                                                        ],
        
                                                                        value=[''],
                                                                        )
                                                                    ],
                                                                    options= [{'label': i, 'value': i} for i in df['Month'].unique()],
                                                                    value=[''],
                                                                    multi=True,
                                                                    searchable=True,
                                                                    placeholder='Filter by Month...',
                                                                    style={'fontSize':15, "border-left":"0px", "border-right":"0px", "border-top":"0px", "border-radius":"0px", "border-color":"teal",
                                                                    'font-family':"'Roboto', sans-serif",'margin': '5px'}
                                                                ),
                                                        ),
                                                    
                                                    dbc.Col(  
                                                        dcc.Dropdown(
                                                                id='table-paging-with-graph-dropdown-product-code',
                                                                options=[
                                                                    {'label': i, 'value': i} for i in df['Product Code'].unique()
                                                                ],
                                                                value=[''],
                                                                multi=True,
                                                                searchable=True,
                                                                placeholder='Filter by Product Code...',
                                                                style={'fontSize':15, "border-left":"0px", "border-right":"0px", "border-top":"0px", "border-radius":"0px", "border-color":"teal",
                                                                    'font-family':"'Roboto', sans-serif",'margin': '5px'}
                                                            ),
                                                            
                                                        ),
                                                    ]),  
                                            ]
                                        ),

                                    ],
                                    style={"width": "100%", "border":"0px", "background-color":"transparent"},
                                ),    
                        ], style={"border":"0px", "background-color":"transparent"} )

    dropdowncard2= dbc.Col([
                            dbc.Card(
                                    [
                                        dbc.CardBody(
                                            [
                                                dbc.Row([
                                                    dbc.Col(
                                                        dcc.Dropdown(
                                                                id='table-paging-with-graph-dropdown-mother-div',
                                                                options=[
                                                                    {'label': i, 'value': i} for i in df['Sales Division'].unique()
                                                                ],
                                                                value=[''],
                                                                multi=True,
                                                                searchable=True,
                                                                placeholder='Filter by Sales Division...',
                                                                style={'fontSize':15, "border-left":"0px", "border-right":"0px", "border-top":"0px", "border-radius":"0px", "border-color":"teal",
                                                                    'font-family':"'Roboto', sans-serif",'margin': '5px'}
                                                            ),
                                                            
                                                        
                                                        ),
                                                    dbc.Col(
                                                        dcc.Dropdown(
                                                                id='table-paging-with-graph-dropdown-sales-div',
                                                                options=[
                                                                    {'label': i, 'value': i} for i in df['Division'].unique()
                                                                ],
                                                                value=[''],
                                                                multi=True,
                                                                searchable=True,
                                                                placeholder='Filter by Division...',
                                                                style={'fontSize':15, "border-left":"0px", "border-right":"0px", "border-top":"0px", "border-radius":"0px", "border-color":"teal",
                                                                    'font-family':"'Roboto', sans-serif",'margin': '5px'}
                                                            ),                                                     
                                                        ),
                                                    
                                                    dbc.Col(  
                                                        dcc.Dropdown(
                                                                id='table-paging-with-graph-dropdown-material-type',
                                                                options=[
                                                                    {'label': i, 'value': i} for i in df['Material Type'].unique()
                                                                ],
                                                                value=[''],
                                                                searchable=True,
                                                                multi=True,
                                                                placeholder='Filter by Material Type...', 
                                                                style={'fontSize':15, "border-left":"0px", "border-right":"0px", "border-top":"0px", "border-radius":"0px", "border-color":"teal",
                                                                    'font-family':"'Roboto', sans-serif",'margin': '5px'}
                                                            ),
                                                            
                                                        ),
                                                    ]),  
                                            ]
                                        ),

                                    ],
                                    style={"width": "100%", "border":"0px", "background-color":"transparent"},
                                ),    
                        ], style={"border":"0px", "background-color":"transparent"} )
    dropdowns = html.Div(
        children=[ 
                    dbc.Card([
                               
                                dbc.CardHeader([
                                    html.Br(),
                                    html.Div( html.Label(" SUMMARY RESULTS "), style={'fontSize':24, 'font-family':"'Roboto', sans-serif", "color": 'teal'}),
                                    html.Br()
                                    ], style={"border":"0px", "background-color":"transparent", "text-align":"center", "justify":"center"}),
                                dbc.CardBody(
                                    [
                                        dbc.Col([
                                                dropdowncard1,
                                                dropdowncard2
                                                
                                            ])
                                   ]
                                ),
                            ],
                            style={"width": "100%", "border":"0px", "background-color":"transparent"},
                    ),
        ])

 
    dashtables =  dbc.Card([
                    dbc.CardHeader([
                                    html.Br(),
                                    html.Div( html.Label(" TABLE "), style={'fontSize':15, 'font-family':"'Roboto', sans-serif", "color": 'teal'}),
                                    html.Br(),
                                    ], style={"border":"0px", "background-color":"transparent", "text-align":"center", "justify":"center"}),
                    html.Div( [     Download(id="download"),
                                    html.Button("EXPORT", id="save-button", 
                                    style={
                                                'font-family':"'Roboto', sans-serif",
                                                'fontSize':15,
                                                "text-decoration": "none",
                                                "color": "#FFF",
                                                "background-color": "#26a69a",
                                                "text-align": "center",
                                                "letter-spacing": ".5px",
                                                "-webkit-transition": ".2s ease-out",
                                                "-moz-transition": ".2s ease-out",
                                                "-o-transition": ".2s ease-out",
                                                "-ms-transition": ".2s ease-out",
                                                "transition": ".2s ease-out",
                                                "box-shadow": "0 0 5px grey",    "border-radius": "5px",
                                                "cursor": "pointer"}) ], style={"border":"0px", "background-color":"transparent",  "text-align":"right", "justify":"center", "margin-right":"15px"}),
                    dbc.CardBody(
                        children=[
                            dash_table.DataTable(
                            id='table-paging-with-graph',
                            columns=[
                                {'name':'Product Code',	'id':'Product Code',	'type':	'text'			      },
                                {'name':'Product Description',	'id':'Product Description',	'type':	'text'},
                                {'name':'Year',	'id':'Year',	'type':	'numeric'                             },
                                {'name':'Date',	'id':'Date',	'type':	'datetime'                            },
                                {'name':'Statistical Forecast',	'id':'Model Forecast',	'type':	'numeric', 'format':Format(decimal_delimiter='.').scheme('f').precision(2)            },
                                {'name':'Actual Forecast',	'id':'Actual Forecast',	'type':	'numeric', 'format':Format(decimal_delimiter='.').scheme('f').precision(2)            },
                                {'name':'Actual Demand',	'id':'Actual Demand',	'type':	'numeric', 'format':Format(decimal_delimiter='.').scheme('f').precision(2)            },
                            ],
                            page_current=0,
                            page_size=20,
                            page_action='custom',
                            sort_action='custom',
                            sort_mode='multi',
                            sort_as_null=['','No'],
                            sort_by=[{'column_id':'Year', 'direction': 'asc' }],
                            style_cell={'padding': '5px', 'fontSize':10, 'font-family':"'Roboto', sans-serif"},
                            style_header={
                                'backgroundColor': '#26a69a',
                                'fontWeight': 'bold', 
                                'color':'white',
                                'fontSize':15, 
                                'font-family':"'Roboto', sans-serif"
                            },
                            style_cell_conditional=[
                                {
                                    'if': {'column_id': c},
                                    'textAlign': 'center'
                                } for c in df.columns
                            ],
                        )

                    ],
                    style={"width": "100%", "border":"0px", "background-color":"transparent"}                    
                    )
                ], style={"border":"0px", "background-color":"transparent"} )


    app.layout = html.Div(
        className="row",
        children=[ 
                    html.Div(
                        children= [
                            dropdowns,
						    dashtables
                        ],
						style={'height': 'auto', 'width':"100%", 'overflowY': 'auto', 'overflowX': 'hidden', 
                        "border":"0px", "background-color":"transparent"},
                    )
                ],
        )

    app.css.append_css({"external_url": os.path.join(assets_path) + "/custom.css"})    

    register_callback(app, df)
        
