#!/usr/bin/env python
import boto3
import json
import write_nodes
from py2neo import Graph, Relationship, authenticate

def main():
    global graph
    authenticate("localhost:7474", "neo4j", "hyenas")
    graph = Graph()
    print(graph.uri)

    s3 = boto3.resource('s3')

    bucket = s3.Bucket('say-bi-prod')
    for item in bucket.objects.all():
        if 'BI_Messages_2016-08-22/' in item.key:
            #msg_file = item.get()['Body'].read()
            msg_file = item.get()['Body']
            print(msg_file)
            json_data = json.load(msg_file)
            #json_data = msg_file
            if 'type' in json_data:
                if json_data['type'] == 'events.quote.QuoteCreated':
                    write_nodes.write_quote_node(graph, json_data)
                elif json_data['type'] == 'events.policy.PolicyCreated':
                    write_nodes.write_policy_node(graph, json_data)
                else:
                    print(json_data['type'])

# Start program
if __name__ == "__main__":
   main()