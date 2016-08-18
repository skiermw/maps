from flask import Flask, request, session, g, redirect, url_for, \
     abort, render_template, flash

from py2neo import Graph, neo4j
import collections

# create our little application :)
app = Flask(__name__)
app.config.from_object(__name__)

# configuration
QUOTE_QUERY = "MATCH (q:Quote) RETURN q.applicant AS applicant, q.lat AS lat, q.lng AS lng"
QUOTE_CT_QUERY = "MATCH (q:Quote) RETURN COUNT(q) AS total"

global graph


@app.route('/')
def show_map():

    graph = Graph()
    total = [result.total for result in graph.cypher.stream(QUOTE_CT_QUERY)]

    lat_lng_query = graph.cypher.execute(QUOTE_QUERY)
    print(lat_lng_query)

    geocodes = [dict(name=result.applicant, lat=result.lat, lng=result.lng) for result in graph.cypher.stream(QUOTE_QUERY)]

    return render_template('show_say.html', geocodes=geocodes, total=total)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5555)
	#app.run()