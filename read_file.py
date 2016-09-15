#!/usr/bin/env python

# this program does a full load of the graph from the BI S3 stored messages.

import boto3
import json
import write_nodes

from py2neo import Graph, Relationship, authenticate

def main():
    global graph
    authenticate("localhost:7474", "neo4j", "shelter")
    graph = Graph()
    #authenticate("http://10.8.30.145:7474/", "neo4j", "shelter")
    #graph = Graph("http://neo4j:shelter@10.8.30.145:7474/db/data/")
    print(graph.uri)

    s3 = boto3.resource('s3',verify=False)

    bucket = s3.Bucket('say-bi-prod')
    for item in bucket.objects.all():
        if 'BI_Messages_' in item.key and not 'BI_Messages_2016-08-18/' in item.key:
            #print(item.key)
            msg_file = item.get()['Body']
            #print(msg_file)
            json_data = json.load(msg_file)
            #json_data = msg_file
            if 'type' in json_data:
                if 'events.quote' in json_data['type']:
                    write_nodes.write_quote_type_node(graph, json_data['type'])
                if json_data['type'] == 'events.quote.QuoteCreated':
                    #print(json_data)
                    write_nodes.write_quote_node(graph, json_data)
                elif json_data['type'] == 'events.policy.PolicyCreated':
                    print('Policy created')
                    print(json_data)
                    write_nodes.write_policy_number_node(graph, json_data)
                elif json_data['type'] == 'events.policy.PolicyOverwritten':
                    print('Policy overwritten')
                    print(json_data)
                    write_nodes.overwrite_policy(graph, json_data)
                else:
                    if 'events.policy' in json_data['type']:
                        print('Found Policy Type %s' % json_data['type'])

# Start program
if __name__ == "__main__":
   main()