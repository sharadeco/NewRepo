#The view or visibility layout of the application
import pandas as pd
import os
import time

from threading import Thread, enumerate
from flask import Flask, Blueprint, render_template, request, flash, url_for, send_from_directory, make_response, current_app, jsonify, Response, session
from werkzeug.utils import secure_filename, redirect

from .transformation import *
from .forecastmodel import *
from .extensions import db, ray

main = Blueprint('main',__name__)

excel_pathhead = current_app.config["DF_TO_EXCEL"]
status = None

def create_temp_location():
    try:
        os.makedirs(current_app.config["DF_TO_EXCEL"], exist_ok=True)
    except:
        os.makedir(current_app.config["DF_TO_EXCEL"], 0o777)

def remove_temp_location():
    import shutil

    try:
        shutil.rmtree(current_app.config["DF_TO_EXCEL"] , ignore_errors=True)
    except OSError as e:
        print("Error: %s : %s" % (current_app.config["DF_TO_EXCEL"] , e.strerror))
        pass



@main.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(current_app.root_path, os.path.normpath("static" + os.sep + "images")),
                               'favicon.ico', mimetype='favicon.ico')



@main.route("/home")
@main.route("/home/")
def home():
    if 'user' in session:
        return render_template('home.html', user=session["user"])
    
    else:
        return redirect(url_for("authorized"))

@main.route("/form-1")
@main.route("/form-1/")
def form1():
    if 'user' in session:
        return render_template('form-1.html', user=session["user"])
    else:
        return redirect(url_for("authorized"))

@main.route("/form-2")
@main.route("/form-2/")
def form2():
    if 'user' in session:
        return render_template('form-2.html', user=session["user"])
    else:
        return redirect(url_for("authorized"))


@ray.remote
def readfile(typename):
    filename = os.path.join(excel_pathhead, "Anios_temp_" + typename.lower() +"_data.csv")
    
    try:
        df= pd.read_csv(filename, encoding="utf-8")
        os.remove(filename)
        return df
    except OSError as e:
        print("Error: %s : %s" % (filename, e.strerror))
        pass
    
    return None


@main.route('/progress')
def progressbar():
    
    mythreads = enumerate()
    flag = None
    blankthread = None

    for thread in mythreads: 
        flag = None
        blankthread = None
        if thread.getName() == "modelthread":
            flag = 1
            blankthread = thread
            break

        elif (thread.getName() == "progressthreads"):
            flag = None
            blankthread = thread
            break

        elif (thread.getName() == "uploadthread"):
            flag = 2
            blankthread = thread
            break

        elif (thread.getName() == "generatesummarythread"):
            flag = 3
            blankthread = thread
            break
        
        else:
            pass

    def generate(flag, threadobj):    
        global status
        if ( status == False or status is None ):
            yield "data:" +str(-20) + "\n\n"

        if flag is not None:
            status = True
            if threadobj.is_alive() and threadobj is not None:

                import random
                val = random.choice(['processing','running','hard at work','working'])

                if flag == 1:
                    print(" ---- Model is running in the background ----- " + threadobj.getName())
                    yield "data: {}".format("Model is " + val + " in the background ") +"\n\n"
            
                elif flag == 2:
                    print(" ---- Upload is running in the background ----- " + threadobj.getName())
                    yield "data: {}".format("Upload is " + val + " in the background ") + "\n\n"

                elif flag == 3:
                    print(" ---- Generate Summary process is running in the background ----- " + threadobj.getName())
                    yield "data: {}".format("Generate Summary process is " + val + " in the background ") + "\n\n"
                
        elif status == True and flag is None:     
            yield "data:" + str(-1) + "\n\n status: Done!" 
            status = False
        
    return Response(generate(flag, blankthread), mimetype= 'text/event-stream')


#The upload files is to load the historical Demand and Forecast data
@main.route('/uploadfiles', methods=['GET','POST'])
def upload():
    if 'user' in session:
        
        if request.method == 'POST':
            #The files are within the requested format
            if request.files:
                file = request.files['filedata']

                if file: 
                    filepath = os.path.join(excel_pathhead, secure_filename(file.filename))
                    file.save(filepath)

                if os.path.getsize(filepath) == 0:
                    message= "Blank File was inserted!"
                    print("[ERROR ]:" + message)
                    flash(message, "danger")
                    
                    return redirect(url_for('main.form1'), code=200) 

                start_time = time.time()
                #If the format of the file is as per request
                if allowed_file(secure_filename(file.filename)):
                    
                    val = str(time.time() - start_time)
                    print("[LOG] FILE PROCESSING TOOK " + val)
                    global status
                    status = False
                    
                    def upload_thread():
                        start_time = time.time()
                        
                        dem_df = readfile.remote("DEMAND")
                        fcs_df = readfile.remote("FORECAST")
                        div_df = readfile.remote("DIVISION")
                        
                        if (dem_df is not None) and (fcs_df is not None) and (div_df is not None):
                            data_demand, data_forecast, data_div =  ray.get([dem_df, fcs_df, div_df])

                        del dem_df
                        del fcs_df
                        del div_df 
                        
                        demand_thread = Thread(name="demand_thread", target=computeDemand, args=(data_demand,data_div))
                        forecast_thread = Thread(name="forecast_thread", target=computeForecast, args=(data_forecast,data_div))      
                        
                        demand_thread.start()
                        forecast_thread.start()
                            
                        
                        demand_thread.join()
                        forecast_thread.join()
                                
                        return 'Done processing'

                    thread_up = Thread(name="uploadthread", target=upload_thread)
                    thread_up.start()

                    progress_thread = Thread(name="progressthreads", target=progressbar)
                    if (thread_up.is_alive()):
                        progress_thread.start()

                else:
                    message = "The file is not in the allowed format"
                    print("[LOG ]"+message)
    
        return redirect(url_for('main.form1'))       

    else:
        return redirect(url_for("authorized"))

@main.route('/rundatamodel', methods=['POST'])
def rundatamodel():    
    
    if 'user' in session:
    
        global status
        status = False

        def process_runmodel(): 
            data = fetch_records('DEMAND')
            start = time.time()

            print("[LOG ]: Started at process at: "+ str(start))
            filename = ray.get(runmodel.remote(data))
            statistical_df = pd.read_csv(filename, encoding="utf-8")
            print(statistical_df)
            computeFCST(statistical_df)        
            del data
            val = time.time()
            print("[LOG ]: MODEL RUN TOOK "+ str(val -start))
            
            del statistical_df
            del val
            del start 
            
            return 'Done running the model'

        model_thread = Thread(name="modelthread", target=process_runmodel)
        model_thread.start()
        progress_thread = Thread(name="progressthreads", target=progressbar)
        if (model_thread.is_alive()):
            progress_thread.start()

        return redirect(url_for('main.form1')) 

    else:
        return redirect(url_for("authorized"))


@main.route('/generatesummary', methods=['POST', 'GET'])
def generatesummary():

    if 'user' in session:
        
        df = fetch_summary_records()
        
        from scpat.dashapp.generate_summary import add_dash as create_generate_summary_dashboard    
        from scpat.dashapp.generate_summary import ui_layout, fetch_details
        
        URL_BASE = fetch_details('URL')
        
        dashapp = create_generate_summary_dashboard("Generate Summary", current_app, URL_BASE)
        ui_layout(dashapp ,df)
        
        return render_template("form-1.html",  dash_url=URL_BASE, user=session['user'])
    
    else:
        return redirect(url_for("authorized"))

@main.route('/generatedashboard', methods=['POST', 'GET'])
def generateform2():
    
    if 'user' in session:
        
        df = fetch_summary_records()
        print(df)
        from scpat.dashapp.dashboard import add_dash as create_form2_dashboard    
        
        from scpat.dashapp.dashboard import ui_layout, fetch_details
       
        
        URL_BASE = fetch_details('URL')
        
        dashapp = create_form2_dashboard("Generate Form2", current_app, URL_BASE)
        ui_layout(dashapp ,df)
        
        return render_template("form-2.html",  dash_url=URL_BASE, user=session['user'])

    else:
        return redirect(url_for("authorized"))



@main.errorhandler(500)
def internal_server_error(e):
    return main.register_error_handler(500, internal_server_error);        time.sleep(50);       redirect(url_for('main.home')) 
