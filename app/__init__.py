from flask import Flask

from flask_bootstrap import Bootstrap

from flask_googlemaps import GoogleMaps

app = Flask(__name__, static_folder='static')

app.config['GOOGLEMAPS_KEY'] = "AIzaSyDajWFzCNwZBG-FDQmNt_nEco0VQGJ7QA0"
GoogleMaps(app)

bootstrap = Bootstrap(app)


from app import views
