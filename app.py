# ___________________________________________________________________________________________________________
# `pip install python-dotenv` (in requirements.txt) allows the FLASK_ENV to be picked from the .env file
# 
# Configurations from the config.py are loaded in app
# 
# 
# 
# .py
# @author: swastika
# ___________________________________________________________________________________________________________


import os
from flask import Flask, Blueprint, render_template, session, request, redirect, url_for, flash
from flask_session import Session  # https://pythonhosted.org/Flask-Session
import msal

from scpat.anios.extensions import db, bt

import ray


def create_app():
    app = Flask(__name__, static_folder='static', template_folder='templates')
    
    with app.app_context():
        if app.config['ENV'] == 'production':
            app.config.from_object('config.ProductionConfig')
        
        elif app.config['ENV'] == 'testing':
            app.config.from_object('config.TestingConfig')
        
        else:
            app.config.from_object('config.DevelopmentConfig')
        
        db.init_app(app)

        from sqlalchemy.orm import scoped_session, sessionmaker
        db.session = scoped_session(sessionmaker(bind=db.engine, autoflush= True, autocommit = False))

        from scpat.anios.main import create_temp_location
        create_temp_location()

        Session(app)

        from werkzeug.middleware.proxy_fix import ProxyFix
        app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

        bt.init_app(app)
        if not ray.is_initialized():  #thread components
            ray.init(num_cpus= os.cpu_count(),
                    include_dashboard=False,
                    object_store_memory=100 * 1024 * 1024)

        from scpat.anios.main import main

        #registers the application blueprint (contains the decorators to route the file)
        app.register_blueprint(main)
        return app


app = create_app()   
app.app_context().push() 


@app.before_first_request        
def before_first_request_func():
    """ 
    This function will run once before the first request to this instance of the application.
    This function is used to create any databases/tables required for application.
    """
    print("The Specifications of the application ")
    print("Database specifications: \n " + str(db.engine) )

    try: 
        statvfs = os.fstatvfs(app.root_path)

        print("Operating system specifications: "
            + "\n CPU count                                  :"+ str(os.cpu_count()) 
            + "\n Size of the filesystem in bytes            : "+ statvfs.f_frsize * statvfs.f_blocks
            + "\n Actual number of free bytes                : "+ statvfs.f_frsize * statvfs.f_bfree
            + "\n No. of available free bytes allowed for use: "+ statvfs.f_frsize * statvfs.f_bavail
            )
    except: 
        print('''This cluster consists of
                {} nodes in total
                {} CPU resources in total
                {} Assigned memory
                {} Object store memory
                '''.format(len(ray.nodes()), ray.cluster_resources()['CPU'], ray.cluster_resources()['memory'], ray.cluster_resources()['object_store_memory'] ))   


@app.teardown_appcontext
def shutdown_session(exception=None):
    ''' Enable Flask to automatically remove database sessions at the
    end of the request or when the application shuts down.
    Ref: http://flask.pocoo.org/docs/patterns/sqlalchemy/
    '''
    
    from threading import enumerate
    mythreads = enumerate()

    try:
        for threads in mythreads:
            threads.join(0.001)
    except:
        pass
    
    db.session.remove()
    ### try to comment when getting error ###


@app.before_request
def before_request_func():
    import gc 
    gc.collect()


@app.route("/")
def index():
    if not session.get("user"):
        flash("Login to access the tool!", "danger")    
        print("Attempted to access without login")    
        return redirect(url_for("login"))
    return redirect('/home')

@app.route("/login")
def login():
    # Technically we could use empty list [] as scopes to do just sign in,
    # here we choose to also collect end user consent upfront
    session["flow"] = _build_auth_code_flow(scopes=app.config['SCOPE'])
    if not session.get("user"):
        return render_template("home.html", auth_url=session["flow"]["auth_uri"])
    else:
        message = "Welcome "+ session["user"] +"!"
        flash(message, "info")
        print("User: "+session["user"]["name"]+" logged in ")    
        return render_template("home.html", user=session["user"])

@app.route(app.config['REDIRECT_PATH'])  # Its absolute URL must match your app's redirect_uri set in AAD
def authorized():
    try:
        cache = _load_cache()
        result = _build_msal_app(cache=cache).acquire_token_by_auth_code_flow(
            session.get("flow", {}), request.args)
        if "error" in result:
            return render_template("auth_error.html", result=result)
        session["user"] = result.get("id_token_claims")
        _save_cache(cache)
    except ValueError:  # Usually caused by CSRF
        pass  # Simply ignore them
    return redirect(url_for("index"))

@app.route("/logout")
def logout():
    session.clear()  # Wipe out user and its token cache from session
    return redirect(  # Also logout from your tenant's web session
        app.config['AUTHORITY'] + "/oauth2/v2.0/logout" +
        "?post_logout_redirect_uri=" + url_for("index", _external=True))


def _load_cache():
    cache = msal.SerializableTokenCache()
    if session.get("token_cache"):
        cache.deserialize(session["token_cache"])
    return cache

def _save_cache(cache):
    if cache.has_state_changed:
        session["token_cache"] = cache.serialize()

def _build_msal_app(cache=None, authority=None):
    return msal.ConfidentialClientApplication(
        app.config['CLIENT_ID'], authority=authority or app.config['AUTHORITY'],
        client_credential=app.config['CLIENT_SECRET'], token_cache=cache)

def _build_auth_code_flow(authority=None, scopes=None):
    return _build_msal_app(authority=authority).initiate_auth_code_flow(
        scopes or [],
        redirect_uri=url_for("authorized", _external=True))

def _get_token_from_cache(scope=None):
    cache = _load_cache()  # This web app maintains one cache per session
    cca = _build_msal_app(cache=cache)
    accounts = cca.get_accounts()
    if accounts:  # So all account(s) belong to the current signed-in user
        result = cca.acquire_token_silent(scope, account=accounts[0])
        _save_cache(cache)
        return result

app.jinja_env.globals.update(_build_auth_code_flow=_build_auth_code_flow)  # Used in template

# -------------------------------------GAURD BLOCK-----------------------------------------------------------
# The `app.run()` function call allows for the application to run the flask application created in the app.py
# Commands that need this: 
# `python app.py` 
#
# Guardblock not needed for :
# `gunicorn app:app`   # default command that works on the azure application
# `python -m flask run --port 5000 --host 0.0.0.0`
# `flask run`

# if __name__ == '__main__': 
#     app.run( host="0.0.0.0", port=5000)
