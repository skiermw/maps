from flask import Flask, request, session, g, redirect, url_for, \
     abort, render_template, flash

from py2neo import Graph, neo4j, authenticate
import collections
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
def index():
    return render_template('index.html')

@app.route('/quote')
def show_quote():
    authenticate("localhost:7474", "neo4j", "hyenas")
    graph = Graph()
    total = [result.total for result in graph.cypher.stream(QUOTE_CT_QUERY)]

    geocodes = [dict(name=result.applicant, lat=result.lat, lng=result.lng) for result in graph.cypher.stream(QUOTE_QUERY)]

    return render_template('quote.html', geocodes=geocodes, total=total[0])

@app.route('/policy')
def show_policy():
    authenticate("localhost:7474", "neo4j", "hyenas")
    graph = Graph()
    total = [result.total for result in graph.cypher.stream(POLICY_CT_QUERY)]

    #lat_lng_query = graph.cypher.execute(QUOTE_QUERY)
    #print(lat_lng_query)

    geocodes = [dict(name=result.applicant, lat=result.lat, lng=result.lng) for result in graph.cypher.stream(POLICY_QUERY)]

    return render_template('show_policies.html', geocodes=geocodes, total=total)

if __name__ == '__main__':
    app.run(debug=True, port=5555)
	#app.run()