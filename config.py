# ___________________________________________________________________________________________________________
# Defines the configuration of the flask application in the config object (picked from .env variable)
# @author: swastika
# ___________________________________________________________________________________________________________

import os
import secrets
import pyodbc
import urllib

#Loading the variables from .env file
from dotenv import load_dotenv
load_dotenv()

config_path = os.path.normpath(os.getcwd() + os.sep)
tmp_excelfilepath = os.path.normpath("tempfiles"+ os.sep)
css_path = os.path.normpath("static"+ os.sep + "css")
js_path = os.path.normpath("static"+ os.sep + "js")

class Config(object):
  
    DEBUG = False
    TESTING = False

    SECRET_KEY = secrets.token_bytes(16)

    driver   = str( os.getenv('DRIVER') )
    user     = str( os.getenv('DB_PROD_USERNAME') )
    server   = str( os.getenv('DB_PROD_SERVERNAME') )
    database = str( os.getenv('DB_PROD_NAME') )
    password = str( os.getenv('DB_PROD_PASSWORD') )


    DB_URL= 'DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+user+';PWD='+ password
    
    DF_TO_EXCEL = os.path.join(config_path, tmp_excelfilepath)
    CSS_PATH = os.path.join(config_path, css_path)
    JS_PATH = os.path.join(config_path, js_path)
    
    #to pass the password as a url quote to avoid delimiting
    SQLALCHEMY_DATABASE_URI = "mssql+pyodbc:///?odbc_connect=%s" % urllib.parse.quote_plus(DB_URL)

    SQLALCHEMY_ECHO=bool(os.getenv('SQLALCHEMY_ECHO'))

    SESSION_COOKIE_SECURE=bool(os.getenv('SESSION_COOKIE_SECURE'))
    MAX_CONTENT_LENGTH=100*1024*1024
    

    SQLALCHEMY_TRACK_MODIFICATIONS=str(os.getenv('SQL_TRACK_MODIFICATIONS'))

    
    CLIENT_ID = "e599afe9-730e-4f77-a9ee-f9019dbf394e" # Application (client) ID of app registration

    CLIENT_SECRET = os.getenv("CLIENT_SECRET")
    if not CLIENT_SECRET:
        raise ValueError("Need to define CLIENT_SECRET environment variable")

    AUTHORITY = "https://login.microsoftonline.com/c1eb5112-7946-4c9d-bc57-40040cfe3a91" 
    # For multi-tenant app
    # AUTHORITY = "https://login.microsoftonline.com/Enter_the_Tenant_Name_Here"

    REDIRECT_PATH = "/getAToken"  # Used for forming an absolute URL to your redirect URI.
                                # The absolute URL must match the redirect URI you set
                                # in the app's registration in the Azure portal.

    # You can find more Microsoft Graph API endpoints from Graph Explorer
    # https://developer.microsoft.com/en-us/graph/graph-explorer
    ENDPOINT = 'https://graph.microsoft.com/v1.0/users'  # This resource requires no admin consent

    # You can find the proper permission names from this document
    # https://docs.microsoft.com/en-us/graph/permissions-reference
    SCOPE = ["User.ReadBasic.All"]
    SESSION_TYPE = "filesystem"  # Specifies the token cache should be stored in server-side session

class ProductionConfig(Config):
    #Insert variables and override for production config
    pass

class DevelopmentConfig(Config):
    DEBUG= True


    driver   = str( os.getenv('DRIVER') )
    user     = str( os.getenv('DB_USERNAME') )
    server   = str( os.getenv('DB_SERVERNAME') )
    database = str( os.getenv('DB_NAME') )
    password = str( os.getenv('DB_PASSWORD') )

    DB_URL= 'DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+user+';PWD='+ password

    #to pass the password as a url quote to avoid delimiting
    SQLALCHEMY_DATABASE_URI = "mssql+pyodbc:///?odbc_connect=%s" % urllib.parse.quote_plus(DB_URL)
   
    
class TestingConfig(Config):
    TESTING= True 

    # DRIVER = Config.DRIVER
    
    DB_USERNAME = os.getenv('DB_USERNAME')
    DB_SERVERNAME = os.getenv('DB_SERVERNAME')
    DB_NAME = os.getenv('DB_NAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
