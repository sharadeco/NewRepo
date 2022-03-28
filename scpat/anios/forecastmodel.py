import sys
import os
import pandas as pd
import numpy as np

from flask import current_app
from .extensions import db, ray
import time


import datetime
import dateutil.relativedelta

now = datetime.datetime.now()
path_head = current_app.config['DF_TO_EXCEL']

########################### segregating low volume data #############################
@ray.remote(memory=10*1024)
def runmodel_low(df):     
    print('[LOG ]: Running forecast model for low volume data')
    df_low=df[(df['mean']<10) | (df['sum']==0)].copy() # this changed 

    print(df_low)
    
    del df

    Mean=pd.DataFrame(pd.DataFrame(df_low['mean']).reset_index().iloc[:,1:])
    df_mean_low=pd.concat([Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean,Mean],axis=1)
    df_low=df_low.transpose().copy()

    df_low=df_low.drop(['mean'],axis=0)
    df_low=df_low.drop(['sum'],axis=0)


    date_range = pd.DataFrame(df_low.index) + pd.DateOffset(months=9)
    date_range1=date_range + pd.DateOffset(months=9)
    date_final=pd.concat([date_range,date_range1],axis=0)
    date_final1=date_final[date_final>df_low.index[(len(df_low.index))-1]]
    date_final1.dropna(inplace=True)

    date_final1=pd.DataFrame(date_final1)
    date_final1.reset_index(inplace=True)
    date_final1=date_final1[0:18:]
    date_final1['Date']


    final_low=pd.DataFrame(df_mean_low.values,columns= date_final1['Date'])   
    final_low=final_low.transpose()
    final_low.columns=list(df_low.columns)

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

    print(df_high)

    del df 

    TimeSeries1=df_high.transpose()
    TimeSeries1.drop(['mean'],inplace=True)
    TimeSeries1.drop(['sum'],inplace=True)

    
    ############################# outlier detection #########################

    ln=len(TimeSeries1.columns)

    cn = 0
    while (cn < ln):

         
         IQR = TimeSeries1.iloc[:,cn].quantile(0.75) - TimeSeries1.iloc[:,cn].quantile(0.25)
         print(IQR)
   
   
         lower_bound = TimeSeries1.iloc[:,cn].quantile(0.25)- (3 * IQR)
         upper_bound = TimeSeries1.iloc[:,cn].quantile(0.75)+ (3 * IQR)

    

         (TimeSeries1.iloc[:,cn][(TimeSeries1.iloc[:,cn] < (lower_bound))]) = np.nan

    
         TimeSeries1.iloc[:,6].fillna(0,inplace=True)
    
         TimeSeries1.iloc[:,cn][(TimeSeries1.iloc[:,cn] > (upper_bound))]=np.nan
         TimeSeries1.iloc[:,cn].fillna(0,inplace=True)
         cn=cn+1
         print("count:",cn)
    
    #TimeSeries1.dropna(how='all', axis=1,inplace=True)    


    ###################  creating the train and validation set  ####################################

    train_data = TimeSeries1[0:]
    plus_month_period = 18

    # pd.date_range(last_month.strftime("%Y-%m-%d"), freq="M", periods=9)
    print(TimeSeries1.index)

    date_range = pd.DataFrame(TimeSeries1.index) + pd.DateOffset(months=9)
    date_range1=date_range + pd.DateOffset(months=9)
    date_final=pd.concat([date_range,date_range1],axis=0)
    date_final1=date_final[date_final>TimeSeries1.index[(len(TimeSeries1.index))-1]]
    date_final1.dropna(inplace=True)

    date_final1=pd.DataFrame(date_final1)
    date_final1.reset_index(inplace=True)
    date_final1=date_final1[0:18:]
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
        test_predictions=holt_winter.predict(n_periods=18)
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
def runmodel(data):
    print('[LOG ]: Started running the forecasting model')
    
    data1=data[['Date','Actuals','Key']].copy()
    data1.dropna(inplace=True)
    
    #data1=data1[data1['Date']>=(now+dateutil.relativedelta.relativedelta(months=-25))]
 
    TimeSeries = data1.pivot_table(index=['Date'],values='Actuals', columns=['Key'],aggfunc='sum')

    del data 
    del data1

    TimeSeries.replace(np.nan,0,inplace=True)
    TimeSeries.index=pd.to_datetime(TimeSeries.index, errors='coerce')
    TimeSeries.index.infer_freq = 'MS' #monthly sequence
    TimeSeries.dropna(how='all', axis=1, inplace=True)
    
    df=TimeSeries.transpose() 
    df['mean']=df.mean(axis=1)
    
    now+dateutil.relativedelta.relativedelta(months=-12)
    df['sum']=TimeSeries[TimeSeries.index >=(now+dateutil.relativedelta.relativedelta(months=-13))].sum()

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

    path = os.path.join(path_head, "Anios_temp_arima_output.csv") 
    final_forecast.to_csv(path, encoding="utf-8")
    
    return path
