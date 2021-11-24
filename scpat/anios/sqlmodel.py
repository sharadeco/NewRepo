from .extensions import db
from datetime import datetime
from flask import current_app
from uuid import uuid4 as unique_id

import json
import pytz

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Anios_DemandData(Base, db.Model):
    __tablename__ = 'Anios_DemandData'
    __table_args__ = {"schema": "dbo"}

    with_timezone_cet = datetime.now(pytz.timezone('CET')).strftime("%Y-%m-%d %H:%M:%S")

    uid= db.Column("id",db.String(20), primary_key=True, default = str(unique_id()))
    key = db.Column("Key",db.String(255), nullable= False)
    code_produit = db.Column("Code Produit", db.String(255), nullable=True)
    produit_designation = db.Column("Désignation", db.String(255), nullable=True)
    divmapcode = db.Column("Sales Division", db.String(255), nullable=True)
    salesdiv = db.Column("Division", db.String(255), nullable=True)    
    produit_fam = db.Column("Famille de produit", db.String(255), nullable=True)
    date = db.Column("Date",db.Date(), nullable=True)
    actuals = db.Column("Actuals",db.Float(), nullable=True, default=0)
    kg = db.Column("KG",db.Float(), nullable=True, default=0)
    delete_ind = db.Column("Delete_Ind",db.String(1), default='F')
    update_timestamp = db.Column("Update_timestamp",db.DateTime(), nullable=True, default=with_timezone_cet)



    def __init__(self, uid, key, code_produit, produit_designation, divmapcode, produit_fam, date, actuals, kg, delete_ind, update_timestamp, salesdiv):
        self.uid = uid
        self.key = key
        self.code_produit = code_produit
        self.produit_designation = produit_designation
        self.divmapcode = divmapcode
        self.produit_fam = produit_fam
        self.date = date
        self.actuals = actuals
        self.kg = kg
        self.delete_ind = delete_ind
        self.update_timestamp = update_timestamp
        self.salesdiv = salesdiv


    def to_dict(self):
        return  {"id":str(self.uid),"Key":self.key,"Code Produit":self.code_produit,"Désignation":self.produit_designation,"Sales Division":self.divmapcode,"Famille de produit":self.produit_fam,"Date":self.date,"Actuals":self.actuals,"KG":self.kg,"Update_timestamp": str(self.update_timestamp), "Division": self.salesdiv}
    
    def __repr__(self):
        return str(self.to_dict())

    def to_json(self):
        return json.dumps(self.to_dict(), ensure_ascii = False, sort_keys=True)
    
class Anios_ForecastData(Base, db.Model):

    __tablename__ = 'Anios_ForecastData'
    __table_args__ = {"schema": "dbo"}

    with_timezone_cet = datetime.now(pytz.timezone('CET')).strftime("%Y-%m-%d %H:%M:%S")

    uid= db.Column("id",db.String(20), primary_key=True, default = str(unique_id()))
    key = db.Column("Key",db.String(255), nullable= False)
    code_produit = db.Column("Code Produit", db.String(255), nullable=True)
    produit_designation = db.Column("Désignation", db.String(255), nullable=True)
    divmapcode = db.Column("Sales Division", db.String(255), nullable=True)
    salesdiv = db.Column("Division", db.String(255), nullable=True)
    produit_fam = db.Column("Famille de produit", db.String(255), nullable=True)
    date = db.Column("Date",db.Date(), nullable=True)
    forecast = db.Column("Forecast", db.Float(), nullable=True, default=0)
    kg = db.Column("KG", db.Float(), nullable=True, default=0)
    delete_ind = db.Column("Delete_Ind",db.String(1), default='F')
    update_timestamp = db.Column("Update_timestamp", db.DateTime(), nullable=True, default=with_timezone_cet)


    def __init__(self, uid, key, code_produit, produit_designation, divmapcode, produit_fam, date, forecast, kg, delete_ind, update_timestamp, salesdiv):
        self.uid = uid
        self.key = key
        self.code_produit = code_produit
        self.produit_designation = produit_designation
        self.divmapcode = divmapcode
        self.produit_fam = produit_fam
        self.date = date
        self.forecast = forecast
        self.kg = kg
        self.delete_ind = delete_ind
        self.update_timestamp = update_timestamp
        self.salesdiv = salesdiv

    def to_dict(self): 
        return {"id":str(self.uid),"Key":self.key,"Code Produit":self.code_produit,"Désignation":self.produit_designation,"Sales Division":self.divmapcode,"Famille de produit":self.produit_fam,"Date":self.date,"Forecast":self.forecast,"KG":self.kg,"Delete_Ind":self.delete_ind,"Update_timestamp": str(self.update_timestamp), "Division": self.salesdiv}

    def __repr__(self): 
        return str(self.to_dict())
    
    def to_json(self):
        return json.dumps(self.to_dict(), ensure_ascii = False, sort_keys=True)

class Anios_CalForecastData(Base, db.Model):

    __tablename__ = 'Anios_CalForecastData'
    __table_args__ = {"schema": "dbo"}

    with_timezone_cet = datetime.now(pytz.timezone('CET')).strftime("%Y-%m-%d %H:%M:%S")
    
    uid= db.Column("id",db.String(20), primary_key=True, default=str(unique_id()))
    key = db.Column("Key",db.String(255), nullable= False)
    code_produit = db.Column("Code Produit", db.String(255), nullable=True)
    produit_designation = db.Column("Désignation", db.String(255), nullable=True)
    divmapcode = db.Column("Sales Division", db.String(255), nullable=True)
    produit_fam = db.Column("Famille de produit", db.String(255), nullable=True)
    date = db.Column("Date",db.Date(), nullable=True)
    statforecast = db.Column("StatForecast", db.Float(), nullable=True, default=0)
    forecast = db.Column("Forecast", db.Float(), nullable=True, default=0)
    kg = db.Column("KG", db.Float(), nullable=True, default=0)
    comments = db.Column("Comments", db.String(255), nullable=True, default='')

    username = db.Column("Username", db.String(255), nullable=True, default='')
    forecast_accuracy = db.Column("Forecast Accuracy", db.Float(), nullable=True, default=0)
    forecast_bias = db.Column("Forecast Bias", db.Float(), nullable=True, default=0)
    salesdiv = db.Column("Division", db.String(255), nullable=True)

    update_timestamp = db.Column(db.DateTime(), nullable=True, default=with_timezone_cet, onupdate=with_timezone_cet)
    delete_ind = db.Column("Delete_Indicator",db.String(1), default='F')

    def __init__(self, uid, key, code_produit, produit_designation, divmapcode, produit_fam, date, statforecast, forecast, kg, comments, update_timestamp, delete_ind, username, fa, fb, salesdiv):
        self.uid = uid
        self.key = key
        self.code_produit = code_produit
        self.produit_designation = produit_designation
        self.divmapcode = divmapcode
        self.produit_fam = produit_fam
        self.date = date
        self.statforecast = statforecast
        self.forecast = forecast 
        self.kg = kg 
        self.comments = comments
        self.update_timestamp = update_timestamp
        #Inserting the calculated forecast as the forecast value that can be later changed
    
        self.delete_ind = delete_ind
        
        self.forecast_accuracy = fa
        self.forecast_bias = fb
        self.username = username
        self.salesdiv = salesdiv
        
    def to_dict(self): 
        return { "id": str(self.uid),"Key":self.key,"Code Produit":self.code_produit,"Désignation":self.produit_designation,"Sales Division":self.divmapcode,"Famille de produit":self.produit_fam,"Date":self.date,"StatForecast":self.statforecast, "Forecast":self.forecast, "KG": self.kg ,"Comments":self.comments ,"Update_timestamp":str(self.update_timestamp), "Delete_Indicator":self.delete_ind, "Forecast Accuracy":self.forecast_accuracy, "Forecast Bias":self.forecast_bias, "Username":self.username, "Division":self.salesdiv}

    def __repr__(self):
        return str(self.to_dict())

    def to_json(self):
        return json.dumps(self.to_dict(), ensure_ascii = False, sort_keys=True)
