import dash
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_core_components as dcc
import dash_table

import pandas as pd 
from flask import current_app, session
import pyodbc
import numpy as np


import datetime
from datetime import datetime
from dash.dependencies import Input, Output, State
import pandas as pd 

import datetime
from dash.exceptions import PreventUpdate
from dash_extensions import Download
from dash.dependencies import Input, Output, State

from scpat.anios.extensions import db
# data drop down react functions

def register_callback(app, df):


    @app.callback(
        Output('table_paging_with_graph_dropdown_unique', 'options'),
        Output('table_paging_with_graph_dropdown_unique', 'value'),
        Input('table_paging_with_graph_dropdown_sales', 'value' ))
    def get_key_options(chosen_input_salesdiv):
       data1 = df
       dff = data1[data1['Division']==chosen_input_salesdiv]
       chosen_input_uid = [{'label': c, 'value': c} for c in sorted(dff['Key'].unique())]
       values_selected = [x['value'] for x in chosen_input_uid]
       return chosen_input_uid, values_selected

    @app.callback(
        Output('table-graph-div', 'children'),
        Output('output-provider','children'),
        Input('table_paging_with_graph_dropdown_unique', 'value'),
        Input('table_paging_with_graph_dropdown_sales', 'value' ))
    def update_filteredtable(input_uid ,input_salesdiv):
        
        if ((input_uid is None) or  (type(input_uid)== list) or  (input_salesdiv is None) ):
            message = " No SKU  or Talentia Div is selected "
            df_filtered = df
            print("Inside if loop")
            print("Input Key is ",input_uid)
            print("Input Sales Div is ",input_salesdiv)
            print(df_filtered.columns)
            consensus1=df_filtered[['Date','Actual Demand','Demand KG', 'Actual Forecast','Final Forecast KG', 'Model Forecast','Actual Forecast KG','Model Forecast KG','Error con','ABS con','Error stat','ABS stat','Final Forecast','Unique Id']]
            Data_grouped=consensus1.groupby('Date').sum()
            Data_grouped['Forecast Bias'] =np.where(Data_grouped['Demand KG']==0,0,Data_grouped['Error con']/Data_grouped['Demand KG'])
            Data_grouped['FA_1']=Data_grouped['ABS con']/Data_grouped['Demand KG']
            Data_grouped['Forecast Accuracy']=np.where(Data_grouped['FA_1']>1,0,1-(Data_grouped['FA_1']))
            Data_grouped['Bias stat'] =np.where(Data_grouped['Demand KG']==0,0,Data_grouped['Error stat']/Data_grouped['Demand KG'])
            Data_grouped['FA_1 Stat']=Data_grouped['ABS stat']/Data_grouped['Demand KG']
            Data_grouped['FA stat']=np.where(Data_grouped['FA_1 Stat']>1,0,1-(Data_grouped['FA_1 Stat']))


            df_filtered=Data_grouped
            df_filtered.reset_index(inplace=True)
            df_filtered.columns
            df_filtered.replace([np.inf, -np.inf], np.nan,inplace=True)
            df_filtered.replace(np.nan,0,inplace=True)
            df_filtered['Forecast Accuracy']=df_filtered['Forecast Accuracy'].round(2)*100
            df_filtered['Forecast Bias']=df_filtered['Forecast Bias']*100
            df_filtered['Forecast Accuracy']=df_filtered['Forecast Accuracy'].round(2)
            df_filtered['Forecast Bias']=df_filtered['Forecast Bias'].round(2)
            df_filtered['FA stat']=df_filtered['FA stat'].round(2)*100
            df_filtered['Bias stat']=df_filtered['Bias stat']*100
            df_filtered['FA stat']=df_filtered['FA stat'].round(2)
            df_filtered['Bias stat']=df_filtered['Bias stat'].round(2)
            df_filtered['Final Forecast KG']= df_filtered['Final Forecast KG'].round(2)
            df_filtered['Model Forecast KG']= df_filtered['Model Forecast KG'].round(2)


        else:
            print("Inside esle loop")
            print("Input Key is ",input_uid)
            print("Input Sales Div is ",input_salesdiv)
            print("Input Key is ",len(input_uid))
            print("Input Key  type is ",type(input_uid))
            print("Input Sales Div is ",len(input_salesdiv))

            #dummy = input_uid.split("*")
            #message = str("Selected SKU : \n Product Code " + dummy[0] + ", Material Type " + dummy[1] )
            #dummy = dummy.partition("*")[2]
            #message = str(message + ", Sales Division " + dummy[2] + ", Division " + dummy[3] )
            message = "Input uid selected is",input_uid
            df_filtered1 = df[df['Division'] == str(input_salesdiv)]
            df_filtered  = df_filtered1[df_filtered1['Key']==str(input_uid)]
        
        dff , dfff = change_table_layout(df_filtered)
        
       
        del dfff
        
        table_div = generate_dash_table(dff)

        return table_div, message

    @app.callback(
        (Output('table-graph-tab', 'children')),   
        [Input('table-paging-with-graph-dropdown-month', 'value'),     
        Input('table-paging-with-graph-dropdown-product-code', 'value'),
        Input('table-paging-with-graph-dropdown-mother-div', 'value'),
        Input('table-paging-with-graph-dropdown-sales-div', 'value'),
        Input('table-paging-with-graph-dropdown-desc', 'value'),
        Input('table-paging-with-graph-dropdown-material-type', 'value'),
        Input('tabs-example', 'value')])
    def update_filtereddashboard(input_month, input_prodcode, input_motherdiv, input_salesdiv, input_desc, input_materialtype, tab):
        
        input_desc = list(filter(None, input_desc))
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
        if len(input_desc) == 0:
            input_desc = df['Product Description']

        df_filtered = df[(df['Month'].isin(input_month)) & (df['Product Description'].isin(input_desc)) & (df['Product Code'].isin(input_prodcode)) & (df['Sales Division'].isin(input_motherdiv)) & (df['Division'].isin(input_salesdiv)) & (df['Material Type'].isin(input_materialtype))]
        dff , dfff = change_table_layout(df_filtered)
        
        del dff        
        
        if tab == 'tab-1':
            graph_div = generate_dash_graph(dfff)
            
        elif tab == 'tab-2':
            graph_div = generate_dash_graph(dfff)

        del dfff

        return graph_div

    @app.callback(
        Output("download", "data"),
        Input("save-button-to-full-csv", "n_clicks"),
        State("table-paging-with-graph", "data"))
    def download_as_csv(n_clicks, table_data):
        
        dff = pd.DataFrame.from_dict({'Unique Id': df['Unique Id'], 'Date': df['Date'], 'Year': df['Year'], 'Month': df['Month'], 'Product Code': df['Product Code'], 
                'Product Description': df['Product Description'], 'Sales Division': df['Sales Division'], 
                'Division': df['Division'], 'Material Type': df['Material Type'], 'Model Forecast '    :  df[ 'Model Forecast' ],'Actual Forecast'    :  df[ 'Actual Forecast'],'Actual Forecast KG'    :  df[ 'Actual Forecast KG'],
                'Commercial Forecast KG'    :  df[ 'Final Forecast KG'    ],'Stat Forecast KG'    :  df['Model Forecast KG'] , 'Actual Demand  '    :  df[ 'Actual Demand'  ],'Demand KG'	: df[ 'Demand KG'      ] })
        if not n_clicks:
            raise PreventUpdate

        import io
        download_buffer = io.StringIO()
        dff.to_csv(download_buffer, index=False)
        
        download_buffer.seek(0)
        
        import datetime

        file= "Anios Consolidated file "+datetime.datetime.today().strftime("%B %Y")+".csv"
        return dict(content=download_buffer.getvalue(), filename=file)

    
    @app.callback(Output('output-provider-2','children'),
        Input('save-button-to-database', 'submit_n_clicks'),
        State('table-paging-with-graph','data'),
        State('table_paging_with_graph_dropdown_unique','value'))
    def update_to_db(submit_n_clicks, table_data, key):

        if not submit_n_clicks:
            raise PreventUpdate
        
        else: 
            now = datetime.datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            blankcsv = pd.DataFrame(table_data)
            blankcsv = blankcsv.set_index('Date').transpose()
            blankcsv.index.name = 'Date'
            blankcsv.reset_index(level=0, inplace= True)
            
            blankcsv.to_csv("table data.csv", header=True)

                       
            today = datetime.datetime.today()
            last_month = datetime.datetime(today.year, today.month, 1 )
            val = pd.date_range(last_month.strftime("%Y-%m-%d"), freq="M", periods=22)
            blankcsv.reset_index(level=0, inplace=True)
            
            blankcsv = blankcsv[blankcsv['Date'].isin(val.strftime("%Y-%m-01").astype(str))]
            
            query = []
            for index, row in blankcsv.iterrows():
                try: 
                    x = format(float(row['Commercial Forecast (ST)']), '.2f')
                except: 
                    x = 0.0
                    count=0
                sql = str("Update dbo.[Anios_CalForecastData] set [Username] = '{}'".format(session['user']['name'])
                #+", [Comments] = '{}' ".format(dt_string)   
                +", [Forecast] = {} ".format(x)
                +"where [Key] = '{}' ".format( key )
                +" and cast([Date] as Date) = '{}'".format(row['Date'])) 


                db.session.execute(sql)


            db.session.commit()

            if len(blankcsv) > 0 and not (key is None or key == ''):
                return " Updated into the database "


    @app.callback(Output('output-provider-3','children'),
        Input('save-consensus-to-database', 'submit_n_clicks'),
        State('table-paging-with-graph','data'),
        State('table_paging_with_graph_dropdown_unique','value'))
    def update_tocon_db(submit_n_clicks, table_data, key):

        if not submit_n_clicks:
            raise PreventUpdate
        
        else: 
            now = datetime.datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            blankcsv = pd.DataFrame(table_data)
            blankcsv = blankcsv.set_index('Date').transpose()
            blankcsv.index.name = 'Date'
            blankcsv.reset_index(level=0, inplace= True)
            
            blankcsv.to_csv("table data.csv", header=True)

                       
            today = datetime.datetime.today()
            last_month = datetime.datetime(today.year, today.month , 1 )
            val = pd.date_range(last_month.strftime("%Y-%m-%d"), freq="M", periods=22)
            blankcsv.reset_index(level=0, inplace=True)
            
            blankcsv = blankcsv[blankcsv['Date'].isin(val.strftime("%Y-%m-01").astype(str))]
            
            query = []
            for index, row in blankcsv.iterrows():
                try: 
                    x = format(float(row['Commercial Forecast (ST)']), '.2f')
                except: 
                    x = 0.0
                    count=0
                sql1 = str("Update dbo.[Anios_CalForecastData] set [Username] = '' "
                #+", [Comments] = '{}' ".format(dt_string)   
                +", [Forecast] = [StatForecast] "
                +"where [Key] = '{}' ".format( key )
                +" and cast([Date] as Date) = '{}'".format(row['Date'])) 

                db.session.execute(sql1)

            db.session.commit()

            if len(blankcsv) > 0 and not (key is None or key == ''):
                return " Updated into the database "
            
                    

#### read data from main


def change_table_layout(df):
    
    a = {'Date': df['Date'].astype(str), 'Unique Id': df['Unique Id'].astype(str), 
        'Statistical forecast (ST)' :  df['Model Forecast' ].astype(float), 'Commercial Forecast (ST)':  df[ 'Final Forecast' ].astype(float),
        'Actual Forecast (ST)'  :  df[ 'Actual Forecast'].astype(float), 'Actual Demand (ST)'    :  df[ 'Actual Demand'  ].astype(float),'Stat Forecast KG':df['Model Forecast KG'].astype(float),
        'Commercial Forecast KG'    :  df[ 'Final Forecast KG' ].astype(float),'Actual Forecast KG':df['Actual Forecast KG'].astype(float),'Demand KG': df[ 'Demand KG'].astype(float), 
        'Forecast accuracy (%)': df['Forecast Accuracy'].astype(float), 'Forecast Bias (%)':df['Forecast Bias'].astype(float),
        'FA Stat (%)': df['FA stat'].astype(float), 'Bias Stat (%)':df['Bias stat'].astype(float),
        }
    #'Comments':df['Comments']
    dff = pd.DataFrame.from_dict(a)

    df_filter = dff.groupby(pd.Grouper(key='Date')).sum().sort_values(by='Date')
    df_filter.reset_index(inplace=True)
    
    #df_filter['Comments'].replace(0.0,'', inplace=True)
    
    dff = df_filter.set_index('Date').transpose()
    dff.index.name = 'Date'
    dff.reset_index(inplace=True, level=0)
    
    
    return dff, df_filter


def generate_dash_graph(dfff):
    print(dfff)

    import plotly.express as px
    import plotly.graph_objects as go
    
    x=dfff["Date"]
    fig = go.Figure()
    for i in ["Demand KG","Commercial Forecast KG"]:
        color = 'black'
        if i == "Demand KG":
            color = 'teal'
        elif i == "Commercial Forecast KG":
            color = 'red'
            
        fig.add_trace(
            go.Bar(
                x=x,
                y=dfff[i],
                name=i,
                text = dfff[i],
                textposition='outside',
                textfont=dict(
                family="'Roboto', sans-serif",
                size=13,
                color=color),
                marker_color=dfff[i],
                marker_line_color=color,
                marker_line_width=2, 
                opacity=0.7
            ))    

    del dfff

    fig.update_layout(
        title=
        {'text':"DASHBOARD",
        'y':1,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'},
        showlegend=True,
        
        xaxis=dict(
            title="DATE",
            titlefont_size=16,
            showgrid = True,
            tickfont_size=8,
            zeroline = True,
            showline = True,
        ),
        yaxis=dict(
            title='AGGREGATED VALUE',
            titlefont_size=16,
            tickfont_size=10
        ),
        bargap=0.5,
        plot_bgcolor="rgb(240,240,240)",
        margin=dict(t=50,l=10,b=10,r=10),        
        legend_title="LEGEND",
        font=dict(
            family="'Roboto', sans-serif",
            size=12,
            color="teal"
        )
    )

    graph_div = dcc.Graph(
                    id='table-paging-graph',
                    figure=fig
                )
    
    return graph_div

def generate_dash_table(dff):
   
    today = datetime.datetime.today()
    last_month = datetime.datetime(today.year, today.month, 1 )
    val = pd.date_range(last_month.strftime("%Y-%m-%d"), freq="M", periods=22)
    
    my_columns = [{'name': ['STATUS', i], 'id': i, 'editable':False, "type":'text'} for i in dff.columns if (i =='Date')]

    my_columns_non_editable = [{'name': ['FIXED', i], 'id': i, 'editable':False, "hideable": "first", "type":'any'} for i in dff.columns if not (i  in val.strftime("%Y-%m-01").astype(str) or i =='Date') ]
    my_columns_editable = [{'name': ['EDITABLE', i], 'id': i, 'editable':True, "type":'any'} for i in dff.columns if i in val.strftime("%Y-%m-01").astype(str)]
    
    for i in my_columns_non_editable:
        my_columns.append(i)

    for i in my_columns_editable:
        my_columns.append(i)

    del my_columns_non_editable
    del my_columns_editable
    del last_month
    def dash_table_conditonal(dff):
         styles = []
         ll=len(dff.columns)
         styles.append({
                                'if': {
                                    'row_index': [1,12],
                                },
                                'backgroundColor': '#6c757d',
                                'color': 'black',
                                'type' : 'text'
                            })
         styles.append({
         
                        'if': {
                            'row_index': [0,2,3,4,5,6,7,8,9,10,11,12],
                        },
                        'color': 'black'

                    })
         for i in range(1,ll):
               if (dff.iloc[1,i].round()!=dff.iloc[0,i].round()):
                   styles.append(
                       {
                          'if': {
                                 'row_index':1,
                                 'column_id':dff.columns[i]
                              },
                          'backgroundColor': '#ffb6c1'
                       }
                       
                       )

         return styles
    
    

    table_div= dash_table.DataTable(
                        id='table-paging-with-graph',
                        columns=my_columns,
                        data= dff.to_dict('records'),
                        editable=True,
                        merge_duplicate_headers=True,
                        export_format="csv",   #exports what is visible on screen
                        style_cell={'padding': '5px', 'fontSize':10, 'font-family':"'Roboto', sans-serif"},
                        style_header={ 'backgroundColor': '#26a69a', 'fontWeight': 'bold', 'color':'white',
                            'fontSize':11, 'font-family':"'Roboto', sans-serif",'text-align':'center'  },
                        style_header_conditional=[
                            {
                                'if': {
                                    'column_id': val.strftime("%Y-%m-01"),
                                },
                                'backgroundColor': 'black',
                                'color': 'white'
                            } ,
                        ],
                        style_data_conditional=dash_table_conditonal(dff)
                            
                    )

    del val

    return table_div

def generate_dropdowns(df):
        
    month_filter= dcc.Dropdown(
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
                                style={'fontSize':10, "border-left":"0px", "border-right":"0px", "width":"100%", "border-top":"0px", "border-radius":"0px", "border-color":"teal",
                                'font-family':"'Roboto', sans-serif",'margin': '5px'}
                            )
    product_filter = dcc.Dropdown(
                                id='table-paging-with-graph-dropdown-product-code',
                                options=[
                                    {'label': i, 'value': i} for i in df['Product Code'].unique()
                                ],
                                value=[''],
                                multi=True,
                                searchable=True,
                                placeholder='Filter by Item...',
                                style={'fontSize':10, "border-left":"0px", "border-right":"0px", "width":"100%", "border-top":"0px", "border-radius":"0px", "border-color":"teal",
                                    'font-family':"'Roboto', sans-serif",'margin': '5px'}
                            )                                
    mother_div_filter= dcc.Dropdown(
                                    id='table-paging-with-graph-dropdown-mother-div',
                                    options=[
                                        {'label': i, 'value': i} for i in df['Sales Division'].unique()
                                    ],
                                    value=[''],
                                        multi=True,
                                    searchable=True,
                                    placeholder='Filter by Selling division...',
                                    style={'fontSize':10, "border-left":"0px", "border-right":"0px", "width":"100%", "border-top":"0px", "border-radius":"0px", "border-color":"teal",
                                        'font-family':"'Roboto', sans-serif",'margin': '5px'}
                            )                                                     
    sales_div_filter= dcc.Dropdown(
                                    id='table-paging-with-graph-dropdown-sales-div',
                                    options=[
                                        {'label': i, 'value': i} for i in df['Division'].unique()
                                    ],
                                    value=[''],
                                    multi=True,
                                    searchable=True,
                                    placeholder='Filter by Mother division...',
                                    style={'fontSize':10, "border-left":"0px", "border-right":"0px", "width":"100%", "border-top":"0px", "border-radius":"0px", "border-color":"teal",
                                        'font-family':"'Roboto', sans-serif",'margin': '5px'}
                            )                                                     
    material_type_filter = dcc.Dropdown(
                                id='table-paging-with-graph-dropdown-material-type',
                                options=[
                                    {'label': i, 'value': i} for i in df['Material Type'].unique()
                                ],
                                value=[''],
                                searchable=True,
                                multi=True,
                                placeholder='Filter by Product type...', 
                                style={'fontSize':10, "border-left":"0px", "border-right":"0px", "width":"100%", "border-top":"0px", "border-radius":"0px", "border-color":"teal",
                                    'font-family':"'Roboto', sans-serif",'margin': '5px'}
                            )
    desc_type_filter = dcc.Dropdown(
                                id='table-paging-with-graph-dropdown-desc',
                                options=[
                                    {'label': i, 'value': i} for i in df['Product Description'].unique()
                                ],
                                value=[''],
                                searchable=True,
                                multi=True,
                                placeholder='Filter by Product Desc...', 
                                style={'fontSize':10, "border-left":"0px", "border-right":"0px", "width":"100%", "border-top":"0px", "border-radius":"0px", "border-color":"teal",
                                    'font-family':"'Roboto', sans-serif",'margin': '5px'}
                            )

    dropdowns = html.Div(
        children=[ 
                    dbc.Card([
                                dbc.CardHeader([
                                            html.Br(),
                                            html.Div( html.Label("SELECT"), style={'fontSize':20, 'font-family':"'Roboto', sans-serif", "color": 'teal'}),
                                            ], style={"background-color":"transparent", "text-align":"center", "justify":"center"}),
                                
                                dbc.CardBody(
                                    [
                                        dbc.Col([
                                                    dbc.Row([material_type_filter], style={"border":"0px","width":"100%"}), 
                                                    dbc.Row([mother_div_filter], style={"border":"0px","width":"100%"}), 
                                                    dbc.Row([sales_div_filter], style={"border":"0px","width":"100%"}),
                                                    dbc.Row([month_filter], style={"border":"0px","width":"100%"}), 
                                                    dbc.Row([product_filter], style={"border":"0px","width":"100%"}) ,    
                                                    dbc.Row([desc_type_filter], style={"border":"0px","width":"100%"})     
                                                ], style={"border":"0px","width":"100%"})

                                    ]
                                ),
                            ],
                            style={"width": "100%", "border":"0px", "background-color":"transparent"},
                    ),
        ])

    return dropdowns

def generate_layout(df):

    dropdowns = generate_dropdowns(df)

    dash_graph_app = dcc.Loading(
                    id="loading-graph", children=[
                        dbc.Card([
                            dbc.CardHeader([
                                        html.Br(),
                                        html.Div( html.Label("GRAPH"), style={'fontSize':20, 'font-family':"'Roboto', sans-serif", "color": 'teal', 'display': 'inline-block', 'width': '20%'}),
                                        ], style={"border":"0px", "background-color":"transparent", "text-align":"center", "justify":"center"}),
                            dbc.CardBody(
                            children=[
                                html.Div(
                                    
                                    children=[
                                        
                                        dcc.Tabs(id='tabs-example', value='tab-1', children=[
                                            dcc.Tab(label='OVERVIEW TAB', value='tab-1'),
                                            dcc.Tab(label='FA/FB TAB', value='tab-2'),
                                        ]),
                                        html.Div(id="table-graph-tab", children=[], style={"float": "center","width":"90%",  
                                            "margin-left":"5%","margin-top":"5%","border":"0px", "background-color":"transparent"})
                                    ])
                            ],
                        style={"width": "100%", "border":"0px", "background-color":"transparent","float": "right"}                    
                        )
                    ], style={"border":"0px", "background-color":"transparent"} )]
                    , type="circle")


    dash_table_app =  dcc.Loading(
                        id="loading-table", children=[
                            
                        dbc.Card([
                            dbc.CardHeader([
                                        html.Br(),
                                        html.Div( html.Label("TABLE"), style={'fontSize':20, 
                                        'font-family':"'Roboto', sans-serif", "color": 'teal', 'background-color':""}),
                                        dbc.Col([
                                            
                                            dbc.Col([
                                                dbc.Row([
                                                    
                                                    html.Div( [     
                                                            Download(id="download"),
                                                            html.Button("EXPORT CONSOLIDATED FILE", id="save-button-to-full-csv", 
                                                            style={"width":"100%", 'fontSize':10, "backgroundColor":"transparent", "border-radius":"0px",
                                                            'font-family':"'Roboto', sans-serif","color":"#555", "border":"1px solid #bbb", "margin-left":"0px" 
                                                            }) 
                                                        ], style={"border":"0px", "background-color":"transparent",  "text-align":"left", "justify":"center", "margin-left":"0px"}
                                                        )
                                                    ]),
                                                dbc.Row([
                                                    html.Div( [  
                                                        dcc.ConfirmDialogProvider(
                                                            children=
                                                            html.Button(
                                                                ['UPDATE TO DATABASE'], 
                                                                style={"width":"100%", 'fontSize':10, "backgroundColor":"transparent", "border-radius":"0px",
                                                                'font-family':"'Roboto', sans-serif","color":"#555", "border":"1px solid #bbb", "margin-left":"0px" 
                                                                }
                                                            ),
                                                            id="save-button-to-database",
                                                            message='Do you want to update changes to database ?'
                                                        )
                                                    ]),
                                                    ]),
                                                dbc.Row([
                                                    html.Div( [  
                                                        dcc.ConfirmDialogProvider(
                                                            children=
                                                            html.Button(
                                                                ['UPDATE CONS WITH STAT'], 
                                                                style={"width":"100%", 'fontSize':10, "backgroundColor":"transparent", "border-radius":"0px",
                                                                'font-family':"'Roboto', sans-serif","color":"#555", "border":"1px solid #bbb", "margin-left":"0px" 
                                                                }
                                                            ),
                                                            id="save-consensus-to-database",
                                                            message='Do you want to update changes to database ?'
                                                        )
                                                    ]),
                                                    ])
                                            
                                            
                                            ])
                                        
                                        ]),
                                       dbc.Col([
                                            dbc.Col([html.Div(
                                                [  "Selling Division",
                                                dcc.Dropdown(
                                                        id='table_paging_with_graph_dropdown_sales',
                                                        options=[
                                                            {'label': i, 'value': i} for i in df['Division'].unique()
                                                        ],
                                                        value='FRS',
                                                            multi=False,
                                                        searchable=True,
                                                        clearable=False,
                                                        placeholder='Filter by Selling division...',
                                                        style={'fontSize':15, "border-left":"0px", "border-right":"0px", "width":"100%", "border-top":"0px", "border-radius":"0px", "border-color":"teal",
                                                            'font-family':"'Roboto', sans-serif",'margin': '5px'}
                                                        )                                                     
                                                ]
                                            ),]),
                                            dbc.Col([html.Div(id='output-provider', style={'fontSize':15, "border-left":"0px", "border-right":"0px", "width":"100%", 
                                                                "border-top":"0px", "border-radius":"0px", "border-color":"teal",
                                                                'font-family':"'Roboto', sans-serif",'margin': '5px'})
                                            ]),
                                            dbc.Col([html.Div(id='output-provider-2', style={'fontSize':15, 
                                                                "border-left":"0px", "border-right":"0px", "width":"100%", 
                                                                "border-top":"0px", "border-radius":"0px", "border-color":"teal",
                                                                'font-family':"'Roboto', sans-serif",'margin': '5px'})
                                            ]),
                                            dbc.Col([html.Div(id='output-provider-3', style={'fontSize':15, 
                                                                "border-left":"0px", "border-right":"0px", "width":"100%", 
                                                                "border-top":"0px", "border-radius":"0px", "border-color":"teal",
                                                                'font-family':"'Roboto', sans-serif",'margin': '5px'})
                                            ])
                                        ]),       
                                       dbc.Col([
                                            dbc.Col([html.Div(
                                                ["Product * Selling Division",
                                                dcc.Dropdown(
                                                        id='table_paging_with_graph_dropdown_unique',
                                                        options=[
                                                             {'label': i, 'value': i} for i in df['Key'].unique()
                                                            ],
                                                            multi=False,
                                                             value=[],
                                                        searchable=True,                                
                                                        placeholder='Filter by Product * Selling division...',
                                                        style={'fontSize':15, "border-left":"0px", "border-right":"0px", "width":"100%", "border-top":"0px", "border-radius":"0px", "border-color":"teal",
                                                            'font-family':"'Roboto', sans-serif",'margin': '5px'}
                                                        )                                                     
                                                ]
                                            ),]),
                                            dbc.Col([html.Div(id='output-provider', style={'fontSize':15, "border-left":"0px", "border-right":"0px", "width":"100%", 
                                                                "border-top":"0px", "border-radius":"0px", "border-color":"teal",
                                                                'font-family':"'Roboto', sans-serif",'margin': '5px'})
                                            ]),
                                            dbc.Col([html.Div(id='output-provider-2', style={'fontSize':15, 
                                                                "border-left":"0px", "border-right":"0px", "width":"100%", 
                                                                "border-top":"0px", "border-radius":"0px", "border-color":"teal",
                                                                'font-family':"'Roboto', sans-serif",'margin': '5px'})
                                            ]),
                                            dbc.Col([html.Div(id='output-provider-3', style={'fontSize':15, 
                                                                "border-left":"0px", "border-right":"0px", "width":"100%", 
                                                                "border-top":"0px", "border-radius":"0px", "border-color":"teal",
                                                                'font-family':"'Roboto', sans-serif",'margin': '5px'})
                                            ])
                                        ]),
                                        
                                        ], style={"border":"0px", "background-color":"transparent", "text-align":"center", "justify":"center"}),
                            dbc.CardBody(
                                        children=[
                                            html.Div(id="table-graph-div", children=[], style={"border":"0px", "background-color":"transparent"})
                                        ],
                                    style={"width": "100%", "border":"0px", "background-color":"transparent","float": "right"}                    
                                    )
                            ], style={"border":"4px", "border-color":"teal","background-color":"transparent"} )
                        ], type="circle"
                )

    return dcc.Loading(
                    id="loading-page", children=[
                    html.Div(
                    children=[ 
                                html.Div(
                                    children= [
                                        
                                        dbc.Row([
                                            dbc.Col([dropdowns], style={"border":"4px", "border-color":"teal"}, width=2), 
                                            dbc.Col([dash_graph_app], style={"border":"4px",  "border-color":"teal"}, width=10)

                                        ], style={"border":"4px", "border-color":"teal"}),
                                        dbc.Row([
                                            dbc.Col([dash_table_app], style={"border":"4px","border-color":"teal"}), 
                                            
                                        ], style={"border":"0px"})
                                            
                                    ],
                                    style={'height': 'auto', 'width':"100%", 'overflowY': 'auto', 'overflowX': 'auto', 
                                    "border":"0px", "background-color":"transparent"},
                                )
                            ],
                    )], 
        type="circle")
