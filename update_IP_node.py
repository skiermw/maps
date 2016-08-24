#!/usr/bin/env python
import requests
import json
import write_nodes
from py2neo import Graph, Relationship, authenticate

def main():
    global graph
    authenticate("localhost:7474", "neo4j", "hyenas")
    graph = Graph()
    print(graph.uri)
    ip = '10.40.1.185'
    ip_node = graph.find_one("IP", "ipAddress", ip)

    url = 'http://ipinfo.io/%s' % ip
    response = requests.get(url)
    if response.status_code != 200:
        print('IP status: %s' % response.status_code)
    else:
        ip_json = json.loads(response.text)
        if 'hostname' in ip_json:
            ip_node['hostname'] = ip_json['hostname']
        if 'city' in ip_json:
            ip_node['city'] = ip_json['city']
        if 'state' in ip_json:
            ip_node['state'] = ip_json['region']
        if 'country' in ip_json:
            ip_node['country'] = ip_json['country']
        if 'loc' in ip_json:
            loc_list = ip_json['loc'].split(',')
            ip_node['lat'] = float(loc_list[0])
            ip_node['lng'] = float(loc_list[1])

    ip_node.push()




# Start program
if __name__ == "__main__":
   main()