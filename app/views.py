from flask import Flask, request, session, g, redirect, url_for, \
     abort, render_template, flash

from py2neo import Graph, neo4j, authenticate
import collections

from flask_googlemaps import GoogleMaps
from flask_googlemaps import Map

from app import app


'''
# create our little application :)
app = Flask(__name__)
app.config.from_object(__name__)
'''
# configuration
QUOTE_QUERY = "MATCH (q:Quote) RETURN q.applicant AS applicant, q.lat AS lat, q.lng AS lng"
QUOTE_CT_QUERY = "MATCH (q:Quote) RETURN COUNT(q) AS total"
POLICY_QUERY = "MATCH (q:Policy) RETURN q.applicant AS applicant, q.lat AS lat, q.lng AS lng"
POLICY_CT_QUERY = "MATCH (q:Policy) RETURN COUNT(q) AS total"

global graph


@app.route('/')
def mapview():
    quote_markers = []
    quote_geocodes = get_quotes()
    total_quotes = len(quote_geocodes)
    for geocode in quote_geocodes:
        marker = {
            'icon': 'http://maps.google.com/mapfiles/ms/icons/blue-dot.png',
            'lat': geocode['lat'],
            'lng': geocode['lng'],
            'infobox': "<b>" + geocode['name'] + "</b>"
        }
        quote_markers.append(marker)
    # creating a map in the view
    quotemap = Map(
        identifier="quote",
        lat=40.4842033386,
        lng=-88.9936904907,
        markers=quote_markers,
        zoom=7,
        style= "height:600px;width:800,px;margin:0;"
    )

    policy_markers = []
    policy_geocodes = get_policies()
    total_policies = len(policy_geocodes)
    for geocode in policy_geocodes:
        marker = {
            'icon': 'http://maps.google.com/mapfiles/ms/icons/green-dot.png',
            'lat': geocode['lat'],
            'lng': geocode['lng'],
            'infobox': "<b>" + geocode['name'] + "</b>"
        }
        policy_markers.append(marker)
    policymap = Map(
        identifier="policy",
        lat=40.4842033386,
        lng=-88.9936904907,
        markers=policy_markers,
        zoom=7,
        style= "height:600px;width:800,px;margin:0;"
    )
    return render_template('maps.html',
                           quotemap=quotemap,
                           policymap=policymap,
                           total_policies=total_policies,
                           total_quotes=total_quotes)

@app.route('/policy')
def policy():

    policy_markers = []
    policy_geocodes = get_policies()
    total_policies = len(policy_geocodes)
    for geocode in policy_geocodes:
        marker = {
            'icon': 'http://maps.google.com/mapfiles/ms/icons/green-dot.png',
            'lat': geocode['lat'],
            'lng': geocode['lng'],
            'infobox': "<b>" + geocode['name'] + "</b>"
        }
        policy_markers.append(marker)
    policymap = Map(
        identifier="policy",
        lat=40.4842033386,
        lng=-88.9936904907,
        markers=policy_markers,
        zoom=7,
        style= "height:600px;width:800,px;margin:0;"
    )
    return render_template('policy.html',
                           policymap=policymap,
                           total_policies=total_policies)


def get_quotes():
    authenticate("localhost:7474", "neo4j", "hyenas")
    graph = Graph()
    geocodes = [dict(name=result.applicant, lat=result.lat, lng=result.lng) for result in graph.cypher.stream(QUOTE_QUERY)]

    return geocodes

def get_policies():
    authenticate("localhost:7474", "neo4j", "hyenas")
    graph = Graph()
    geocodes = [dict(name=result.applicant, lat=result.lat, lng=result.lng) for result in graph.cypher.stream(POLICY_QUERY)]

    return geocodes

if __name__ == '__main__':
    app.run(debug=True, port=5555)
	#app.run()