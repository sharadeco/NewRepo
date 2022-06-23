import sys
import os
import pandas as pd
import numpy as np
import dateutil
from flask import current_app
from .extensions import db, ray
import time
from datetime import datetime,date
from dateutil.relativedelta import relativedelta
from dateutil import rrule



import datetime

now = datetime.datetime.now()
path_head = current_app.config['DF_TO_EXCEL']

########################### segregating low volume data #############################
@ray.remote(memory=10*1024)
def runmodel_low(df):     
    print('[LOG ]: Running forecast model for low volume data')
    df_low=df[(df['mean']<10) | (df['sum']==0)].copy() # this changed 
    
    del df

    Mean=pd.DataFrame(pd.DataFrame(df_low['mean']).reset_index().iloc[:,1:])
    df_mean_low=pd.concat([Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean],axis=1)
    df_low=df_low.transpose().copy()

    df_low=df_low.drop(['mean'],axis=0)
    df_low=df_low.drop(['sum'],axis=0)
    print(df_low.columns)

    date_range = pd.DataFrame(df_low.index) + pd.DateOffset(months=9)
    date_range1=date_range + pd.DateOffset(months=9)
    date_range2=date_range1 + pd.DateOffset(months=9)
    date_final=pd.concat([date_range,date_range1,date_range2],axis=0)
    date_final.drop_duplicates(inplace=True)
    date_final1=date_final[date_final>df_low.index[(len(df_low.index))-1]]
    date_final1.dropna(inplace=True)
    date_final1['Date'] = pd.to_datetime(date_final1['Date'], format='%Y-%m-%d')
    
    date_final1=pd.DataFrame(date_final1)
    date_final1.reset_index(inplace=True)
    date_final1=date_final1[0:22:]
    date_final1['Date']
    print("Date columns are as follows ",date_final1)

    final_low=pd.DataFrame(df_mean_low.values,columns= date_final1['Date'])   
    final_low=final_low.transpose()
    final_low.columns=list(df_low.columns)
    print(final_low.columns)

    del Mean
    del date_range
    del date_range1
    del df_mean_low
    del df_low

    return final_low

########################### segregating high volume data #############################
@ray.remote(memory=1024*1024)
def runmodel_high(df):     
    print('[LOG ]: Running forecast model for high volume data')
    df_high=df[(df['mean']>=10) & (df['sum']!=0)].copy()


    del df 

    TimeSeries1=df_high.transpose()
    TimeSeries1.drop(['mean'],inplace=True)
    TimeSeries1.drop(['sum'],inplace=True)


    ###################  creating the train and validation set  ####################################

    train_data = TimeSeries1[0:]

    date_range = pd.DataFrame(TimeSeries1.index) + pd.DateOffset(months=9)
    date_range1=date_range + pd.DateOffset(months=9)
    date_range2=date_range1 + pd.DateOffset(months=9)
    date_final=pd.concat([date_range,date_range1,date_range2],axis=0)
    date_final.drop_duplicates(inplace=True)
    date_final1=date_final[date_final>TimeSeries1.index[(len(TimeSeries1.index))-1]]
    date_final1.dropna(inplace=True)
    date_final1['Date'] = pd.to_datetime(date_final1['Date'], format='%Y-%m-%d')

    date_final1=pd.DataFrame(date_final1)
    date_final1.reset_index(inplace=True)
    date_final1=date_final1[0:22:]
    date_final1['Date']

    test_data=date_final1['Date']
    
    ############################ AUTO ARIMA ############################################
    ## automatically defines value of I to give the best fit for the model

    ln1=len(TimeSeries1.columns)

    result=[]
    test_predictions=[]

    for i in range(0,ln1):    
        from pmdarima.arima import auto_arima

        X= (train_data.iloc[:,i].astype('float'))
    
        holt_winter=auto_arima(X,trace=True,
                            error_action='ignore',  
                            suppress_warnings=True, 
                            stepwise=True,scoring='mse')
        holt_winter.fit(X)
        test_predictions=holt_winter.predict(n_periods=22)
        result.append(list(test_predictions))
        print("count=",i)

    del ln1
    del test_predictions
    del train_data
    del holt_winter
    del date_range
    del date_range1

    final=pd.DataFrame(result,columns=test_data)    
    pred=final.transpose()
    pred[pred<0]=0
    pred=pred.round()
    pred.columns=list(TimeSeries1.columns)

    del TimeSeries1
    del test_data
    del result
    del final
    
    return pred    


@ray.remote(memory=10*1024)
def runmodel(data,New_data):
    print('[LOG ]: Started running the forecasting model')
    
    data1=data[['Code Produit','Division','Sales Division','Actuals', 'Famille de produit','Date','Key']].copy()
   
    print(data1)
    data1.dropna(inplace=True)
    data2=data1[data1['Date']>=(now+dateutil.relativedelta.relativedelta(months=-25))]
    data2.groupby(['Key','Date','Code Produit','Division','Sales Division', 'Famille de produit'])
    print(data2)
    data2['freq']=np.where(data2['Actuals']>0,1,0)
    data2=data2[data2['Actuals']!=0]
    print(data2)


    Unique_data=data2[['Key','Code Produit','Division','Sales Division', 'Famille de produit']].copy()
    Unique_data = Unique_data.drop_duplicates()
    Unique_data=Unique_data.merge(data2.groupby('Key')['freq'].sum() , on='Key', how='inner')
    Unique_data['category1']=np.where(Unique_data['freq']<3,'Extremely slow','Categorized demand')
    Unique_data['status1']=23/Unique_data['freq']
    data2['Actuals']=data2['Actuals'].astype('int')
    Unique_data=Unique_data.merge(data2.groupby('Key')['Actuals'].std() , on='Key', how='inner')
    Unique_data['category2']=np.where(Unique_data['status1']<10,'Non Outlier','Outlier')
    ### std deviation , average deviation , cv

    Unique_data=Unique_data.merge(data2.groupby('Key')['Actuals'].std(), on='Key', how='inner')
    Unique_data.rename(columns={'Actuals_x':'Std dev','Actuals_y':'Std1'},inplace=True)
    Unique_data=Unique_data.merge(data2.groupby('Key')['Actuals'].mean(), on='Key', how='inner')
    Unique_data.rename(columns={'Actuals':'Avg dev'},inplace=True)
    Unique_data['CV']=Unique_data['Std dev']/Unique_data['Avg dev']

    Unique_data['category3']=np.where(Unique_data['category1']=='Extremely slow','Extremely slow',np.where(Unique_data['status1']<1.32,"Non Intermittent","Intermittent"))
    Unique_data['category4'] = np.where(Unique_data['CV']<0.49,"Non Intermittent-Smooth","Non Intermittent-Erratic")
    Unique_data['category5'] = np.where(Unique_data['Std dev']<4,"Intermittent-Low variable","Intermittent-High variable")
    print(Unique_data)

    ################################# Outlier detection  ##############################

    Status1=Unique_data[['Key', 'category2', ]]

    Outlier_data=pd.merge(data1,Status1,on ='Key', how = 'left')

    Outlier_data1=Outlier_data[Outlier_data['category2']=='Outlier']
    NonOutlier_data=Outlier_data[Outlier_data['category2']!='Outlier']

    Outlier_data2 = Outlier_data1.pivot_table(index=['Date'],values='Actuals', columns=['Key'],aggfunc='sum')
    Outlier_data2.replace(np.nan,0,inplace=True)
    Outlier_data2.index=pd.to_datetime(Outlier_data2.index, errors='coerce')
    Outlier_data2.index.infer_freq = 'MS'
    Outlier_data2.dropna(how='all', axis=1, inplace=True)
    print(Outlier_data2)


    ln=len(Outlier_data2.columns)
    cn = 0


    while (cn < ln):

         
              IQR = Outlier_data2.iloc[:,cn].quantile(0.75) - Outlier_data2.iloc[:,cn].quantile(0.25)
              print(IQR)
   
   
              lower_bound = Outlier_data2.iloc[:,cn].quantile(0.25)- (3 * IQR)
              upper_bound = Outlier_data2.iloc[:,cn].quantile(0.75)+ (3 * IQR)

    

              (Outlier_data2.iloc[:,cn][(Outlier_data2.iloc[:,cn] < (lower_bound))]) = np.nan

    
              Outlier_data2.iloc[:,6].fillna(0,inplace=True)
    
              Outlier_data2.iloc[:,cn][(Outlier_data2.iloc[:,cn] > (upper_bound))]=np.nan
              Outlier_data2.iloc[:,cn].fillna(0,inplace=True)
              cn=cn+1
              print("count:",cn)
    
    Outlier_data2.dropna(how='all', axis=1,inplace=True)    



    Outlier_data2=Outlier_data2.transpose()
    Outlier_data2 = Outlier_data2.unstack().reset_index(name='Actuals')
    Outlier_data2[['Code Produit','Famille de produit','Division','Sales Division']]=Outlier_data2['Key'].str.split('*',expand=True)
    NonOutlier_data.drop(['category2'],axis=1,inplace=True)

    data_final=pd.concat([Outlier_data2,NonOutlier_data],axis=0)
    data_final=data_final[data_final['Sales Division'].isin(['FRS','HEALTHCARE', 'INSTITUTIONAL', 'LIFE SCIENCES'])]

    Status=Unique_data[['Key', 'category3' ]]
    Status_data=pd.merge(data_final,Status,on ='Key', how = 'left')
    Status_data=Status_data.dropna(how='all')

    Status_data=Status_data[Status_data['category3']!="Extremely slow"]
    Status_data['category3'].replace(np.nan,"out",inplace=True)
    Status_data=Status_data[Status_data['category3']!="out"]
    Status_data.drop(['category3'],axis=1,inplace=True)
    print(Status_data)
    #data1=data1[data1['Date']>=(now+dateutil.relativedelta.relativedelta(months=-25))]
 
    TimeSeries = Status_data.pivot_table(index=['Date'],values='Actuals', columns=['Key'],aggfunc='sum')

    del data 
    del data1
    del data2
    del Outlier_data1,Outlier_data2
    del NonOutlier_data

    TimeSeries.replace(np.nan,0,inplace=True)
    TimeSeries.index=pd.to_datetime(TimeSeries.index, errors='coerce')
    TimeSeries.index.infer_freq = 'MS' #monthly sequence
    TimeSeries.dropna(how='all', axis=1, inplace=True)
    
    df=TimeSeries.transpose() 
    df['mean']=df.mean(axis=1)
    print(df)

    now+dateutil.relativedelta.relativedelta(months=-12)
    df['sum']=TimeSeries[TimeSeries.index >=(now+dateutil.relativedelta.relativedelta(months=-25))].sum()

    del TimeSeries


    df_low = runmodel_low.remote(df)
    df_high = runmodel_high.remote(df)

    if (df_low is not None) and (df_high  is not None) :
        final_low, final_high = ray.get([ df_low, df_high])

    del df_low
    del df_high

    final_forecast=pd.concat([final_low, final_high],axis=1)

    final_forecast=final_forecast.transpose().copy()
    final_forecast.index.name='Key'

    def myFun(New_data,final_forecast):

        if New_data.empty:
            test2 = final_forecast
        else:

            df =New_data[''].astype(str).str.split("-", expand=True)
            df.rename(columns={0:"Key",1:"Index_new"},inplace=True)
            df.fillna(0,inplace=True)
            df.replace('',0,inplace=True)
            df.drop("Index_new",axis=1,inplace=True)
            df= df.drop_duplicates()
            print(df)

            #start_date=datetime.datetime.now().replace(day=1).replace(minute=00, hour=00, second=00)
            #end_date=datetime.datetime.now().replace(day=1).replace(minute=00, hour=00, second=00) + relativedelta(months=21)
    
            list_date = final_forecast.columns
   
            for x in list_date:
             df[x]=''
        
            df.reset_index()
            df.set_index("Key",inplace=True)
            print(" df Columns are as below *****************  ",df.columns)
            print("fc Columns are as below *****************  ",final_forecast.columns)
    
    
            test2= pd.concat([final_forecast,df],axis=0)
        return test2
    

    df_new =myFun(New_data,final_forecast)
    df_new.reset_index(inplace=True)
    df_new=df_new.drop_duplicates(inplace=True)
    df_new=df_new.drop_duplicates('Key', keep='first')
    df_new.set_index("Key",inplace=True)

    df_new.fillna(0,inplace=True)
    df_new.replace('',0,inplace=True)
    path = os.path.join(path_head, "Anios_temp_arima_output.csv") 
    df_new.to_csv(path, encoding="utf-8")
    
    return path
