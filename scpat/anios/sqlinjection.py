from .extensions import db,db1, ray
from .sqlmodel import *
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import and_, or_, not_, extract, insert
from flask import flash,  url_for, current_app
from datetime import date, datetime
from uuid import uuid4 as unique_id


import pytz
import os
import time
import json
import pandas as pd
from filelock import FileLock

#Json loads is crucial as it makes the object serializable
        
def checkdbsession():

    import pyodbc
    import pandas as pd
    import numpy as np
    server = 'gsc-scpat-sql-001-d.database.windows.net'
    database = 'SC-PAT-DB'
    username = 'SCPAT'
    password = 'Ecolab@1234'
    cnxn1 = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn1.cursor()
    return cursor
    #if db is None: 
    #    db.init_app(current_app)

    #if db.engine is None: 
    #    from sqlalchemy import create_engine
    #    db.engine = create_engine(current_app.config['SQLALCHEMY_DATABASE_URI'])

    #if db.session is None:
    #    from sqlalchemy.orm import scoped_session, sessionmaker
    #    db.session = scoped_session(sessionmaker(bind=db.engine, autoflush= True, autocommit = False))

    #try: 
    #    check = db.session.connect()
    #except: 
    #    check = db.session.connection()
    #if ( check is None):
    #    db.session.connection = db.engine.connect()
    
#inserting the Demand data
#This process memory is restricted for use
@ray.remote(memory=1024)
def convert_demand_to_json(df, pobj):
    
    without_timezone = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with_timezone_utc = datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
    with_timezone_cet = datetime.now(pytz.timezone('CET')).strftime("%Y-%m-%d %H:%M:%S")

    if df.empty == False:
        count =0
        json_obj = open(pobj,'a+', encoding='utf-8')
        json_obj.seek(0,0)
        with FileLock(pobj + ".lock"):
            for index, row in df.iterrows():
                data=Anios_DemandData(unique_id(), row['Key'], row['Code Produit_x'], row['Désignation'], row['Code Produit_y'], row['Famille de produit'], 
                                        row['Date'].strftime('%Y-%m-%d'), row['Qté Fact'], row['Kg sold'], 'F', with_timezone_cet, row['Division'])
                
                # print("Demand record:", data)
                json.dump(json.loads(data.to_json()), json_obj, ensure_ascii=False)
                json_obj.write("\n")
                count+= 1

            message = "Parsed "+str(count)+ " Demand records in the database"
            print("[INFO ]:"+message)
            return True


    #Data type demand is not present in the Data tab
    else: 
        message = "'Data type' demand is missing in the sheet!"
        print("[ERROR ]:"+message)
        return None




@ray.remote(memory=1024)
def convert_fcst_to_json(df, flag, pobj):

    without_timezone = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with_timezone_utc = datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
    with_timezone_cet = datetime.now(pytz.timezone('CET')).strftime("%Y-%m-%d %H:%M:%S")

    if df.empty == False:
        count=0
        json_obj = open(pobj,'a+', encoding='utf-8')
        json_obj.seek(0,0)
        with FileLock(pobj + ".lock"):
            for index, row in df.iterrows():
                if flag == True:
                    data=Anios_ForecastData(unique_id(), row['Key'], row['Code Produit_x'], row['Désignation'], row['Code Produit_y'], row['Famille de produit'], 
                                    row['Date'].strftime('%Y-%m-%d'), row['Qté Fact'], row['Kg sold'], "F", with_timezone_cet, row['Division'])
                    # print("Forecast record ", data)
                else:
                    data=Anios_CalForecastData(unique_id(), row['Key'], row['Code Produit'], "DUMMY", row['Division Mapping#Code Produit'], row['Famille de produit'],
                                     row['Date'].strftime('%Y-%m-%d'), row['Qté Fact'], row['Qté Fact'], row['Qté Fact'], "", with_timezone_cet, "F", '', 0, 0, row['Sales Div'])
                    print("Updated rows to database ")
                
                json.dump(json.loads(data.to_json()), json_obj, ensure_ascii=False)
                json_obj.write("\n")
                count+=1    
            
            if flag == True:
                message = "Parsed "+str(count)+ " Forecast records in the database"

            else:
                message = "Parsed "+str(count)+ " calculated Forecast records in the database"

            print("[INFO ]:"+message)
            return True
    
    #Data type forecast is not present in the Data tab
    else:
        if flag == True:
            message = "'Data type' forecast is missing in the sheet!"

        else:
            message = "'Data type' forecast is not calculated in the sheet!"

        print("[ERROR ]:"+message)
        return None

def insert_to_db_from_json(filename,type):
    
    if type == 'DEMAND':    
        update_records("DELETE_INDICATOR", "Demand")
        stmt = insert(Anios_DemandData)
        values_list = []
        with open(filename,encoding='utf-8') as r:
            for data in r:
                obj = json.loads(data)
                values_list.append(obj)

            db.session.execute(stmt, values_list)
            db.session.commit()
            message = "Loaded "+ str(len(values_list)) +" actual demand value in the database."
            print("[INFO ]:"+message)            
            del stmt
            del values_list

        r.close()

    elif type == 'FCST':
        update_records("DELETE_INDICATOR", "Forecast") 
        stmt = insert(Anios_ForecastData)
        values_list = []
        with open(filename,encoding='utf-8') as r:
            for data in r:
                obj = json.loads(data)
                values_list.append(obj)

            db.session.execute(stmt, values_list)
            db.session.commit()
            message = "Loaded "+ str(len(values_list)) +" actual forecast value in the database."
            print("[INFO ]:"+message)            
            del stmt
            del values_list

      
        r.close()
        
    else:
        update_records("DELETE_INDICATOR", "Calculated")
        stmt = insert(Anios_CalForecastData)
        values_list = []
        with open(filename,encoding='utf-8') as r:
            for data in r:
                obj = json.loads(data)
                values_list.append(obj)

            db.session.execute(stmt, values_list)
            db.session.commit()
            message = "Loaded "+ str(len(values_list)) +" statistical forecast value in the database."
            print("[INFO ]:"+message)
            
            del stmt
            del values_list

        r.close()
        update_records("Product Description", "Calculated")
        
    return True    

    from sqlalchemy.orm import scoped_session, sessionmaker

def fetch_records(datatype):
    start = time.time()
    print("[LOG ]: Started at :"+ str(start))

    sql_timezone = str("DECLARE @datevar_IST as datetime = CAST(SWITCHOFFSET(SYSDATETIMEOFFSET(), '+05:30') AS DATETIME), "   #-- IST is GMT+05:30 
                        +"@datevar_CET as datetime = CAST(SWITCHOFFSET(SYSDATETIMEOFFSET(), '+02:00') AS DATETIME)		  ")  #-- CET is GMT+02:00 
    
    if datatype == 'DEMAND' : #fetching the Demand Data from the tables 
        sql = str( sql_timezone
                       +"SELECT [id]" 
                       + ",[Code Produit] "
                       + ",[Désignation] "
                       + ",[Division Mapping#Code Produit]"
                       + ",[Famille de produit]"
                       + ",[Date]"
                       + ",[Actuals]"
                       + ",[KG]"
                       + ",[Sales Div]"
                       + ",[key] as [Key] "
                       + " FROM [dbo].[Anios_DemandData] "
                       + " where delete_ind = 'F'"
                       + " and (datepart(year, @datevar_CET) - datepart(year, [Date])) <=3")
        
        
    elif datatype == 'FORECAST': #fetching the Forecast Data from the tabless
        sql = str( "SELECT [id]" 
                       + ",[Code Produit] "
                       + ",[Désignation] "
                       + ",[Division Mapping#Code Produit]"
                       + ",[Famille de produit]"
                       + ",[Date]"
                       + ",[Forecast]"
                       + ",[KG]"
                       + ",[Sales Div]"
                       + ",[key] as [Key] "
                       + " FROM [dbo].[Anios_ForecastData] "
                       + " where delete_ind = 'F'")

    elif datatype == 'CALCULATED': #fetching the Calculated Data from the tables
        sql = str( "SELECT [id]" 
                       + ",[Code Produit] "
                       + ",[Désignation] "
                       + ",[Division Mapping#Code Produit]"
                       + ",[Famille de produit]"
                       + ",[Date]"
                       + ",[StatForecast]"
                       + ",[Forecast]"
                       + ",[Comments]"
                       + ",[KG]"
                       + ",[key] as [Key] "
                       + " FROM [dbo].[Anios_CalForecastData] "
                       + " where delete_indicator = 'F'")

    elif datatype == 'SUMMARY': #fetching the entire SUMMARY that is generated using the view dbo.Anios_SummaryDetails
        sql = str( " select newid() as [id],       "
                    +" [Unique Id],                  "
                    +" [Key],                        "
                    +" [Date],                       "
                    +" [Year],                       "
                    +" [Month],                      "
                    +" [Product Code],               "
                    +" [Sales Div],                  "
                    +" [Product Description],        "
                    +" [Mother Division],            "
                    +" [Material Type] ,             "
                    +" [Actual Demand],             "
                    +" [Demand KG],                  "
                    +" [Actual Forecast],            "
                    +" [Forecast KG],                "
                    +" [Model Forecast],             "
                    #+" [Comments],                   "
                    +" [Forecast Accuracy],          "
                    +" [Forecast Bias],              "
                    +" [User],                       "
                    +" [Final Forecast],             "
                    +" [Update_timestamp],           "
                    +" [Delete_Ind]                  "
                    +" from dbo.Anios_SummaryDetails "
                )
                #print ("Date Variable",@datevar_CET)
    else: #fetching the product descriptions from the tables
        pass

    import pyodbc
    import pandas as pd
    import numpy as np
    print(checkdbsession())
    cursor = checkdbsession()
    cursor.execute(sql)
    row = cursor.fetchall()
    field_name1 = [field[0] for field in cursor.description]

    ######### converting list of list into matrix and then into dataframe

    LOL1=np.matrix(row)
    dataframe=pd.DataFrame(LOL1)
    dataframe.columns=field_name1    
    cursor.close()    

    #try:
    #    data = db.session.execute(sql)
    #except:
    #    db.session.rollback()
    #    data = db.session.execute(sql)    
    #data1 = [row[0] for row in data]
    
    #print(" data is ",data1)
    #from pandas import DataFrame
    #dataframe = DataFrame(data.fetchall())
    #dataframe.columns = data.keys()
    #dataframe= pd.read_sql_query(sql, con= connection, index_col=['id'])
    
    return dataframe


def update_records(column, table): 
    
    sql_timezone = str("DECLARE @datevar_IST as datetime = CAST(SWITCHOFFSET(SYSDATETIMEOFFSET(), '+05:30') AS DATETIME), "   #-- IST is GMT+05:30 
                          +"@datevar_CET as datetime = CAST(SWITCHOFFSET(SYSDATETIMEOFFSET(), '+02:00') AS DATETIME)		  ")  #-- CET is GMT+02:00 

    if column == "DELETE_INDICATOR":
    
       
        #this is to prevent duplicate entries, majorly for Demand and Forecast Data
        if table == "Demand":
            sql = str( sql_timezone                    
                    +"Update dbo.[Anios_DemandData] set dbo.[Anios_DemandData].Delete_Ind = 'T', 						 "
                    +"dbo.[Anios_DemandData].[Update_timestamp] = @datevar_CET                                           "
                    +"WHERE DATEPART(year, dbo.[Anios_DemandData].[Update_timestamp]) <= datepart(year,@datevar_CET) AND  "
                    +"DATEPART(month, dbo.[Anios_DemandData].[Update_timestamp])	<= datepart(month, @datevar_CET) AND  "
                     #---------------------------Only overwrite the last month and upcoming data------------------------- 
                    +"dbo.[Anios_DemandData].[Date]		    >= DATEADD(MONTH, -13,  @datevar_CET) AND  "
                    # +"DATEPART(month, dbo.[Anios_DemandData].[Date])		    >= datepart(month,@datevar_CET)- 1) AND  "
                    +"dbo.[Anios_DemandData].[Delete_Ind] = 'F'	 ")
            
            sqlD = str( sql_timezone                    
                    +"Delete from dbo.[Anios_DemandData] where [Date]  >= DATEADD(MONTH, -13,  @datevar_CET)")
            db.session.execute(sqlD)
            
        if table == "Forecast":
            sql = str( sql_timezone
                    +"Update dbo.[Anios_ForecastData] set dbo.[Anios_ForecastData].Delete_Ind = 'T', "
                    +"dbo.[Anios_ForecastData].[Update_timestamp] = @datevar_CET "
                    +"WHERE DATEPART(year, dbo.[Anios_ForecastData].[Update_timestamp]) <= datepart(year,@datevar_CET) AND "
                    +"DATEPART(month, dbo.[Anios_ForecastData].[Update_timestamp])    <= datepart(month, @datevar_CET) AND "
                    
                    #---------------------------Only overwrite the upcoming data----------------------------------------
                    +"((DATEPART(year, dbo.[Anios_ForecastData].[Date])		    = datepart(year,@datevar_CET) 	     AND  "
                    +" DATEPART(month, dbo.[Anios_ForecastData].[Date])		    >= datepart(month,@datevar_CET))      OR  "
                    +" (DATEPART(year, dbo.[Anios_ForecastData].[Date])			> datepart(year,@datevar_CET) 	     AND  "
                    +" DATEPART(month, dbo.[Anios_ForecastData].[Date])		    <= datepart(month,@datevar_CET)) )   AND  "
                    +"dbo.[Anios_ForecastData].[Delete_Ind] = 'F'     ") 

        if table == "Calculated": 
            sql = str(sql_timezone
                    +"Update dbo.[Anios_CalForecastData] set dbo.[Anios_CalForecastData].Delete_Indicator = 'T', "
                    +"dbo.[Anios_CalForecastData].[Update_timestamp] = @datevar_CET "
                    +"WHERE DATEPART(year, dbo.[Anios_CalForecastData].[Update_timestamp]) <= datepart(year,@datevar_CET) AND "
                    +"DATEPART(month, dbo.[Anios_CalForecastData].[Update_timestamp])    <= datepart(month, @datevar_CET) AND "
                    
                    #---------------------------Only overwrite the upcoming data----------------------------------------
                    +"((DATEPART(year, dbo.[Anios_CalForecastData].[Date])		    = datepart(year,@datevar_CET) 	     AND  "
                    +" DATEPART(month, dbo.[Anios_CalForecastData].[Date])		    >= datepart(month,@datevar_CET))      OR  "
                    +" (DATEPART(year, dbo.[Anios_CalForecastData].[Date])			> datepart(year,@datevar_CET) 	     AND  "
                    +" DATEPART(month, dbo.[Anios_CalForecastData].[Date])		    <= datepart(month,@datevar_CET)) )   AND  "
                     +"dbo.[Anios_CalForecastData].[Delete_Indicator] = 'F'        ")             
            
        
        
        data = db.session.execute(sql)
         
    #The following values are applicable only for the Calculated forecast table
    elif column == "Product Description":
        sql = str( sql_timezone
                +"UPDATE A "
                +"SET  "
                +"    A.[Désignation]=B.[Désignation],  "
                +"    A.[Update_Timestamp]= @datevar_CET "
                +"FROM dbo.Anios_CalForecastData A "
                +"INNER JOIN "
                +"dbo.Anios_DemandData  B "
                +"ON A.[Code Produit] = B.[Code Produit] " )

        data = db.session.execute(sql)
    
    print("[LOG ]: Executing  "+ sql)
    
    del sql_timezone
    del sql
    del data
    
    db.session.commit()
    print("[LOG ]: Updated "+ column +" values for "+ table +" table ")
    return True
